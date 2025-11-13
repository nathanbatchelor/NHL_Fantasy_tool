"""
seed_player_map.py

Improved seeder that:
- Always prioritizes DB players (only DB players are written)
- Indexes ESPN players more aggressively (team.roster, free agents, attempted statuses)
- Includes rostered IR players explicitly
- Uses deterministic fallbacks and a fuzzy-name fallback to catch minor name differences
- Logs reasons for match / mismatch for quick debugging
"""

import pandas as pd
from sqlmodel import Session
from database import engine
from models.database import PlayerMap
import connect_espn
import constants
import unicodedata
import re
from difflib import get_close_matches

# --- Configuration ---

QUERY_DB_PLAYERS = """
    SELECT player_id, player_name, team_abbrev
    FROM player_game_stats
    WHERE player_name IS NOT NULL

    UNION

    SELECT player_id, player_name, team_abbrev
    FROM goalie_game_stats
    WHERE player_name IS NOT NULL
"""

# --- Helpers ---


def strip_accents(s: str) -> str:
    nfkd_form = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))


def clean_name_for_split(name: str) -> str:
    """Lowercase, normalize whitespace, remove hyphens, keep initials together (jt -> jt)."""
    if not name:
        return ""
    name = name.strip().lower()
    name = name.replace("-", " ")
    # Insert space if compressed initial like "d.jiricek" -> "d. jiricek"
    name = re.sub(r"([a-z])\.([a-z])", r"\1. \2", name)
    # Remove dots completely so "j.t." -> "jt"
    name = name.replace(".", "")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def build_name_forms(name: str):
    """
    Produce:
      - primary_key: first initial + last (jmiller)
      - extended_key: all initials / first part + last (jtmiller or jamessurname if full)
      - cleaned_full: cleaned 'first last' with accents removed (for fuzzy)
    """
    cleaned = clean_name_for_split(name)
    if not cleaned:
        return "", "", ""

    parts = cleaned.split()
    if len(parts) == 1:
        last = strip_accents(parts[0])
        return last, last, last

    first_part = "".join(
        [c for c in parts[0] if c.isalpha()]
    )  # can be 'j' or 'jt' or 'james'
    last = "".join(parts[1:])
    last = strip_accents(last)
    first_part = strip_accents(first_part)

    primary = f"{first_part[0]}{last}"  # j + miller
    extended = (
        f"{first_part}{last}" if len(first_part) > 1 else primary
    )  # jt + miller or james + miller
    cleaned_full = f"{first_part} {last}"

    primary = re.sub(r"[\s\-]", "", primary)
    extended = re.sub(r"[\s\-]", "", extended)
    cleaned_full = re.sub(r"\s+", " ", cleaned_full).strip()

    return primary, extended, cleaned_full


# --- Fetchers ---


def get_db_players() -> dict:
    """
    Return mapping: primary_key -> (nhl_id, actual_name)
    Deduplicate by player_id (keep last).
    """
    print("Fetching players from DB...")
    with engine.connect() as conn:
        df = pd.read_sql(QUERY_DB_PLAYERS, conn)

    # Dedupe by player_id so a traded player doesn't create multiple DB entries
    if "player_id" in df.columns:
        df = df.drop_duplicates(subset=["player_id"], keep="last")

    df["primary_key"] = df["player_name"].apply(lambda n: build_name_forms(n)[0])
    # keep DB canonical name when duplicates by primary_key appear (rare)
    player_map = {}
    for _, row in df.iterrows():
        player_map[row["primary_key"]] = (row["player_id"], row["player_name"])

    print(f"Found {len(player_map)} unique DB players (primary keys).")
    return player_map


def get_espn_players(league) -> dict:
    """
    Return a dictionary mapping many keys -> espn_id, and a reverse map id -> canonical name.
    Keys include primary and extended forms and the cleaned full name.
    Values are single ints or lists if collisions occur; collisions are preserved.
    """
    print("Indexing ESPN players (rosters + free agents + extra statuses)...")
    espn_map = {}
    espn_id_to_name = {}

    def add_key(key: str, espn_id: int):
        if not key:
            return
        existing = espn_map.get(key)
        if existing is None:
            espn_map[key] = espn_id
        else:
            # convert to list if collision with different id
            if isinstance(existing, int):
                if existing != espn_id:
                    espn_map[key] = [existing, espn_id]
            else:
                if espn_id not in existing:
                    existing.append(espn_id)

    def index_player_obj(p):
        # p is espn_api.hockey.Player
        primary, extended, cleaned_full = build_name_forms(p.name)
        add_key(primary, p.playerId)
        add_key(extended, p.playerId)
        add_key(cleaned_full.replace(" ", ""), p.playerId)  # "jt miller" -> "jtmiller"
        espn_id_to_name[p.playerId] = p.name

    # rostered players (includes IR on team roster)
    for team in league.teams:
        for p in team.roster:
            index_player_obj(p)

    # attempt to include free agents and waivers if available
    # try a larger size; wrap in try/except because some leagues or versions may differ
    for status in (None, "FREEAGENT", "WAIVERS"):
        try:
            if status is None:
                # some espn_api versions accept league.free_agents() with no args
                players = league.free_agents(size=3000)
            else:
                players = league.free_agents(size=3000, status=status)
            for p in players:
                index_player_obj(p)
        except TypeError:
            # older/newer versions of espn_api might not accept status param
            try:
                players = league.free_agents(size=3000)
                for p in players:
                    index_player_obj(p)
            except Exception:
                # give up this status quietly
                pass
        except Exception:
            # catch other failure but continue
            pass

    print(f"Indexed ~{len(espn_map)} distinct keys from ESPN (approx).")
    return espn_map, espn_id_to_name


# --- Matching logic (DB-first) ---


def resolve_collision(val):
    """If val is int -> return it; if list -> deterministic pick first and log."""
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, list) and val:
        # deterministic: choose first but warn — DB-first policy ensures we'll only accept if DB player exists
        print(
            f"⚠️ ESPN key collision: multiple ESPN IDs {val} — choosing first deterministically."
        )
        return val[0]
    return None


def fuzzy_match_db_to_espn(db_cleaned_full: str, espn_keys: list[str], cutoff=0.86):
    """
    Use difflib.get_close_matches on cleaned names (no spaces) to find candidate ESPN key.
    Returns key or None.
    """
    if not db_cleaned_full:
        return None
    # prepare candidates without spaces to maximize match potential
    candidates = [
        k for k in espn_keys if k
    ]  # espn keys are already without spaces in many cases
    # try direct match first
    if db_cleaned_full in candidates:
        return db_cleaned_full
    # use close matches on strings (we'll operate on no-space forms)
    matches = get_close_matches(db_cleaned_full, candidates, n=3, cutoff=cutoff)
    return matches[0] if matches else None


def main():
    db_map = get_db_players()
    league = connect_espn.connect()
    if not league:
        print("Could not connect to ESPN. Exiting.")
        return

    espn_map, espn_id_to_name = get_espn_players(league)
    if not espn_map:
        print("Could not index ESPN players. Exiting.")
        return

    espn_keys_list = list(espn_map.keys())

    print(
        "\nMatching DB players (DB-first). Will attempt deterministic and fuzzy fallbacks.\n"
    )
    matched = 0
    unmatched = 0
    newly_matched_via_fuzzy = []

    with Session(engine) as session:
        for db_key, (nhl_id, db_name) in db_map.items():
            # Build name forms from DB canonical name
            primary, extended, cleaned_full = build_name_forms(db_name)
            cleaned_nospace = cleaned_full.replace(" ", "")

            match_reason = None
            espn_id = None

            # 1) direct primary key
            val = espn_map.get(primary)
            espn_id = resolve_collision(val)
            if espn_id:
                match_reason = f"primary key '{primary}'"

            # 2) extended key (e.g., J.T vs J.)
            if not espn_id:
                val = espn_map.get(extended)
                espn_id = resolve_collision(val)
                if espn_id:
                    match_reason = f"extended key '{extended}'"

            # 3) cleaned_full nospace (e.g., 'jtmiller')
            if not espn_id:
                val = espn_map.get(cleaned_nospace)
                espn_id = resolve_collision(val)
                if espn_id:
                    match_reason = f"cleaned full '{cleaned_nospace}'"

            # 4) last-name suffix scan (endswith) — safe: DB-first ensures we only accept if DB player exists
            if not espn_id:
                candidate = (
                    next((k for k in espn_keys_list if k.endswith(primary[1:])), None)
                    if len(primary) > 1
                    else None
                )
                if candidate:
                    espn_id = resolve_collision(espn_map.get(candidate))
                    if espn_id:
                        match_reason = f"suffix scan match '{candidate}'"

            # 5) fuzzy fallback (closest cleaned_no_space)
            if not espn_id:
                fuzzy_key = fuzzy_match_db_to_espn(
                    cleaned_nospace, espn_keys_list, cutoff=0.86
                )
                if fuzzy_key:
                    espn_id = resolve_collision(espn_map.get(fuzzy_key))
                    if espn_id:
                        match_reason = f"fuzzy match '{fuzzy_key}'"
                        newly_matched_via_fuzzy.append(
                            (db_name, primary, fuzzy_key, espn_id)
                        )

            # Finally, check avoid list
            if espn_id and espn_id in getattr(constants, "AVOID_PLAYER_ESPN_IDS", []):
                print(f"Skipping {db_name} because ESPN ID {espn_id} is in avoid list.")
                espn_id = None
                match_reason = None

            if espn_id:
                espn_name = espn_id_to_name.get(espn_id, "Unknown")
                print(
                    f"✓ {db_name} (NHL:{nhl_id}) ↔ ESPN:{espn_id} [{espn_name}]  -- matched by {match_reason}"
                )
                session.merge(
                    PlayerMap(nhl_id=int(nhl_id), espn_id=espn_id, player_name=db_name)
                )
                matched += 1
            else:
                print(
                    f"✗ {db_name} (no ESPN match) -- tried keys: primary='{primary}', extended='{extended}', cleaned='{cleaned_nospace}'"
                )
                unmatched += 1

        session.commit()

    print("\n" + "=" * 50)
    print(" PLAYER MAP SEEDING COMPLETE")
    print("=" * 50)
    print(f"  ✓ Matched:   {matched}")
    print(f"  ✗ Unmatched: {unmatched}")

    if newly_matched_via_fuzzy:
        print(
            "\nNote: the following DB entries were only matched via fuzzy fallback (inspect):"
        )
        for db_name, primary, fuzzy_key, espn_id in newly_matched_via_fuzzy:
            print(
                f"  - {db_name} (primary={primary}) -> fuzzy_key={fuzzy_key} -> ESPN:{espn_id} [{espn_id_to_name.get(espn_id)}]"
            )

    print("\nOnly DB players were written. Fuzzy matches are logged above for review.")
    print(
        "If many players remain unmatched, run the script with smaller free_agent size increments or inspect unmatched primary keys."
    )


if __name__ == "__main__":
    main()

# TODO: if a new player gets added, we need to automatically detect this in our daily script and add them to the player_map
