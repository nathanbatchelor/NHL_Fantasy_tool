# NHL Fantasy Tool 🏒 | AI-Powered Fantasy Hockey Assistant

> ⚠️ **Data Science Showcase:** Demonstrates data engineering, machine learning pipelines, and asynchronous API integration for fantasy sports analytics.

-----

## 🔥 Project Overview

**NHL Fantasy Tool** is a comprehensive CLI-based data engineering and machine learning solution designed to give fantasy hockey managers a competitive edge. It automates the ingestion of real-time NHL statistics, manages local fantasy rosters, and uses predictive modeling to forecast player performance.

By aggregating historical data, opponent strength, and recent form, the tool removes the guesswork from setting lineups and identifying waiver wire gems. Built with **Python 3.13**, **SQLModel**, and **Scikit-Learn**.

### Core Problem Solved:

Eliminates "gut feeling" decisions. Managers can:

1.  **Automate Data Collection:** Daily fetch of game logs, boxscores, and schedules.
2.  **Predict Performance:** AI-driven point projections for upcoming matchups.
3.  **Manage Rosters:** Track specific fantasy teams, process trades, and handle drops.
4.  **Analyze Waiver Wire:** Instantly identify "hot" free agents based on advanced metrics.
5.  **Optimize Lineups:** Make data-backed decisions on who to start or sit.

-----

## Core Concepts Demonstrated

This project showcases advanced Python development and data science skills:

### **Data Engineering & Backend:**

  * **Async Pipelines:** High-concurrency data fetching using `asyncio`, `httpx`, and `tenacity` for robust API interactions with retries.
  * **Data Persistence:** Relational data modeling with **SQLModel** (SQLAlchemy + Pydantic) backed by **SQLite**.
  * **Incremental Updates:** Efficient logic to fetch and merge only new game data (`PlayerStatsProcessor`), preventing redundant processing.
  * **Caching:** JSON-based caching layer to reduce API load and speed up development cycles.

### **Machine Learning (Scikit-Learn):**

  * **Feature Engineering:** Transformation of raw stats into predictive features (Rolling averages L3/L5, Days Rest, Opponent Strength, Home/Away splits).
  * **Modeling:** Training a **Random Forest Regressor** to predict specific fantasy point outcomes.
  * **Evaluation:** Model performance tracking using Mean Absolute Error (MAE) and R-squared metrics.

### **CLI & Tooling:**

  * **Interactive CLI:** Scripts for managing teams, running predictions, and analyzing players directly from the terminal.
  * **Modular Architecture:** Separation of concerns between API clients, database models, core logic, and executable scripts.

-----

## 🚀 Features Implemented

### 🤖 **AI Player Predictions**

  * **ML Pipeline:** `build_training_set.py` generates features -\> `train_model.py` trains the Random Forest -\> `run_predictions.py` generates forecasts.
  * **Context Aware:** Predictions account for opponent defensive stats, back-to-back games, and player position.
  * **Integration:** Writes predicted scores directly to the database for easy querying.

### 📊 **Daily Data Pipeline**

  * **Automated Updates:** `update_daily_stats.py` fetches yesterday's games and updates player records incrementally.
  * **Smart Seeding:** Scripts to seed past game data and prior season stats to build a robust historical dataset.
  * **Robust Fetching:** Handles API rate limits and errors gracefully with exponential backoff.

### 🛠️ **Fantasy Team Management**

  * **Roster Control:** Create leagues, add/drop players, and execute trades via `manage_fantasy_teams.py`.
  * **Matchup Analysis:** `get_remaining_week_matchups.py` calculates schedule density for the current fantasy week.
  * **Waiver Wire:** `waiver_wire.py` runs complex SQL queries to find top-performing free agents over the last 14 days.

### 📈 **Player Analysis**

  * **Deep Dives:** `player_analyzer.py` provides a detailed report on a specific player, including seasonal baselines, recent form, and opponent difficulty for the current week.

-----

## 🏗️ Architecture

The project is structured as a modular Python application:

  * **`src/api`**: Async clients for the NHL API, handling response parsing and Pydantic validation.
  * **`src/database`**: SQLModel definitions (`ProPlayers`, `PlayerGameStats`, `GoalieGameStats`) and DB connection logic.
  * **`src/core`**: Configuration, scoring weights, and team mappings.
  * **`scripts/`**: The executable entry points for the user (ETL jobs, ML training, CLI tools).

### **Data Flow:**

`NHL API` → `Async Fetcher` → `JSON Cache` → `Pydantic Models` → `SQLite DB` → `Pandas DataFrame` → `ML Model`

-----

## 🛠️ Tech Stack

### **Core**

  * **Language:** Python 3.13+
  * **Database:** SQLite
  * **ORM:** SQLModel (SQLAlchemy + Pydantic)

### **Data Science**

  * **Analysis:** Pandas, NumPy
  * **Machine Learning:** Scikit-Learn (RandomForestRegressor)
  * **Serialization:** Joblib

### **Utilities**

  * **Network:** HTTPX (Async HTTP), Tenacity (Retries)
  * **Environment:** Python-dotenv
  * **Scheduling:** Asyncio

-----

## 📂 Project Structure

```
nhl-fantasy-tool/
├── data/                      # SQLite DB and JSON Cache files
├── models/                    # Trained ML models (.pkl)
├── scripts/                   # Executable scripts
│   ├── run_predictions.py     # Generate AI predictions
│   ├── update_daily_stats.py  # ETL job for daily updates
│   ├── train_model.py         # ML training pipeline
│   ├── manage_fantasy_teams.py# Roster management CLI
│   └── ...
├── src/                       # Source code
│   ├── api/                   # API Client & Response Models
│   ├── database/              # Database Models & Utils
│   ├── core/                  # Constants & Config
│   └── utils/                 # Helper functions
└── requirements.txt           # Dependencies
```

-----

## 🚦 Usage

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Initialize & Seed Data:**
    ```bash
    python -m scripts.seed_past_game_data
    python -m scripts.seed_prior_season_stats
    ```
3.  **Manage Teams:**
    ```bash
    python -m scripts.manage_fantasy_teams
    ```
4.  **Train Model & Predict:**
    ```bash
    python -m scripts.build_training_set
    python -m scripts.train_model
    python -m scripts.run_predictions
    ```

-----

## 📞 Contact

  * **GitHub:** [https://github.com/nathanbatchelor](https://github.com/nathanbatchelor)
  * **Email:** nathanbatchelor04@gmail.com

-----

## ⚖️ License & Copyright

**© [2025] [Nathan Batchelor]. All Rights Reserved.**

This source code is available for viewing purposes only. No part of this code may be used, modified, or distributed for any purpose without explicit written permission from the owner.

*Last Updated: November 2025*
