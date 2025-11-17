import unicodedata
import re

# --- Helper Functions (Copied from seed_player_map.py) ---
# These are needed for generate_player_key to work.


def strip_accents(s: str) -> str:
    nfkd_form = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))


def clean_name_for_split(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    name = name.replace("-", " ")
    name = re.sub(r"([a-z])\.([a-z])", r"\1. \2", name)
    name = name.replace(".", "")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def build_name_forms(name: str):
    cleaned = clean_name_for_split(name)
    if not cleaned:
        return ""

    parts = cleaned.split()
    if len(parts) < 2:
        # Handle single-name cases or empty strings
        return strip_accents(cleaned)

    # First part is the first name/initial(s), last part is the last name.
    first_name_part = parts[0]
    last_name_part = parts[-1]

    # Get the very first character of the first name part.
    first_initial = strip_accents(first_name_part)[0]

    # The last name is the last part of the split name.
    last_name = strip_accents(last_name_part)

    # This creates the 'jmiller' or 'alee' part
    key = f"{first_initial}{last_name}"
    key = re.sub(r"[\s\-]", "", key)  # Should already be clean, but as a safeguard

    return key


# --- Function to Test ---


def generate_player_key(name: str, position: str) -> str:
    """
    Generates a unique key like 'alee-L' from:
    Name: 'A. Lee'
    Position: 'Left Wing'
    """
    if not name or not position:
        return ""

    # Use the existing helper to get the 'alee' part
    key = build_name_forms(name)

    # Get the 'L' part from 'Left Wing'
    pos_initial = position[0].upper()

    return f"{key}-{pos_initial}"


# --- Test Runner ---


def run_tests():
    """
    Defines and runs a set of test cases for generate_player_key.
    """
    print("Testing generate_player_key function...")

    # Define test cases: (name, position, expected_output)
    test_cases = [
        # Standard cases
        ("A. Lee", "Left Wing", "alee-L"),
        ("Connor McDavid", "Center", "cmcdavid-C"),
        ("Igor Shesterkin", "Goalie", "ishesterkin-G"),
        ("Cale Makar", "Defense", "cmakar-D"),
        # Names with hyphens
        ("Jean-Gabriel Pageau", "Center", "jpageau-C"),
        ("Pierre-Luc Dubois", "Center", "pdubois-C"),
        # Names with initials
        ("J.T. Miller", "Center", "jmiller-C"),
        # Names with accents
        ("Sébastien Aho", "Center", "saho-C"),
        ("Artturi Lehkonen", "Right Wing", "alehkonen-R"),
        # Names with suffixes (tests the `parts[-1]` logic)
        ("Kieffer Bellows Jr.", "Left Wing", "kjr-L"),
        ("Kieffer Bellows", "Left Wing", "kbellows-L"),
        # Edge cases: Single names (based on `build_name_forms` logic)
        ("Nico", "Center", "nico-C"),
        # Edge cases: Empty inputs
        ("", "", ""),
        ("Player Name", "", ""),
        ("", "Position", ""),
    ]

    passed_count = 0
    failed_count = 0

    for i, (name, position, expected) in enumerate(test_cases):
        actual = generate_player_key(name, position)

        if actual == expected:
            print(f"  ✓ PASS: ({name}, {position}) -> '{actual}'")
            passed_count += 1
        else:
            print(f"  ✗ FAIL: ({name}, {position})")
            print(f"    - Expected: '{expected}'")
            print(f"    - Got:      '{actual}'")
            failed_count += 1

    print("\n--- Test Summary ---")
    print(f"Total: {len(test_cases)}, Passed: {passed_count}, Failed: {failed_count}")
    if failed_count == 0:
        print("All tests passed!")


if __name__ == "__main__":
    run_tests()
