"""
Movie Data Ingestion Script
Fetches movie data from OMDB API and saves to local JSON files.
"""

import requests
import json
import time
import os

# ============================================================
# CONFIGURATION - UPDATE THESE
# ============================================================
API_KEY = "your_omdb_api_key_here"  # ⚠️ Paste your OMDB key here!
OUTPUT_FOLDER = "movie_json"
BASE_URL = "http://www.omdbapi.com/"

# ============================================================
# LIST OF MOVIES TO FETCH (100 popular movies)
# ============================================================
MOVIES = [
    "The Godfather", "The Shawshank Redemption", "The Dark Knight",
    "Pulp Fiction", "Schindler's List", "Forrest Gump", "Inception",
    "Fight Club", "The Matrix", "Goodfellas", "The Lord of the Rings: The Return of the King",
    "Star Wars: Episode V - The Empire Strikes Back", "Interstellar",
    "Spirited Away", "Parasite", "Whiplash", "The Departed",
    "Gladiator", "The Prestige", "The Lion King", "Back to the Future",
    "Saving Private Ryan", "The Silence of the Lambs", "Avengers: Endgame",
    "Joker", "Titanic", "The Avengers", "Avatar", "Jurassic Park",
    "The Wolf of Wall Street", "Django Unchained", "Inglourious Basterds",
    "The Truman Show", "Memento", "Casablanca", "Citizen Kane",
    "Vertigo", "Psycho", "Rear Window", "North by Northwest",
    "Singin' in the Rain", "12 Angry Men", "Seven Samurai", "Spartacus",
    "Lawrence of Arabia", "2001: A Space Odyssey", "A Clockwork Orange",
    "The Shining", "Apocalypse Now", "Taxi Driver", "Raging Bull",
    "Scarface", "Heat", "Reservoir Dogs", "Kill Bill: Vol. 1",
    "Kill Bill: Vol. 2", "No Country for Old Men", "There Will Be Blood",
    "The Social Network", "Birdman", "Mad Max: Fury Road",
    "Get Out", "La La Land", "Moonlight", "Arrival", "Dunkirk",
    "Tenet", "Oppenheimer", "Everything Everywhere All at Once",
    "Top Gun: Maverick", "Spider-Man: Into the Spider-Verse", "Coco",
    "Inside Out", "Up", "Toy Story", "Finding Nemo", "Wall-E",
    "Ratatouille", "The Incredibles", "Monsters, Inc.", "Frozen",
    "Shrek", "Beauty and the Beast", "Aladdin", "The Little Mermaid",
    "Mulan", "Tangled", "Zootopia", "Spider-Man: No Way Home",
    "Iron Man", "Black Panther", "Guardians of the Galaxy",
    "Thor: Ragnarok", "Doctor Strange", "Captain America: The Winter Soldier",
    "Logan", "Deadpool", "X-Men: Days of Future Past",
    "The Batman", "Dune", "Blade Runner 2049", "Mission: Impossible - Fallout",
    "John Wick"
]

# ============================================================
# CREATE OUTPUT FOLDER
# ============================================================
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)
    print(f"Created folder: {OUTPUT_FOLDER}")

# ============================================================
# FETCH EACH MOVIE
# ============================================================
all_movies = []
success_count = 0
fail_count = 0

print(f"\nStarting to fetch {len(MOVIES)} movies...\n")

for i, title in enumerate(MOVIES, 1):
    print(f"[{i}/{len(MOVIES)}] Fetching: {title}")
    
    try:
        params = {
            "t": title,
            "apikey": API_KEY,
            "plot": "short"
        }
        response = requests.get(BASE_URL, params=params, timeout=10)
        data = response.json()
        
        if data.get("Response") == "True":
            all_movies.append(data)
            success_count += 1
        else:
            print(f"  ⚠️ Not found: {title} - {data.get('Error', 'Unknown error')}")
            fail_count += 1
        
        # Be nice to the API - small delay
        time.sleep(0.1)
    
    except Exception as e:
        print(f"  ❌ Error: {e}")
        fail_count += 1

# ============================================================
# SAVE ALL MOVIES TO ONE JSON FILE
# ============================================================
output_file = os.path.join(OUTPUT_FOLDER, "movies.json")

# Save as JSON Lines format (one movie per line) - better for big data tools
with open(output_file, "w", encoding="utf-8") as f:
    for movie in all_movies:
        f.write(json.dumps(movie) + "\n")

print(f"\n{'='*50}")
print(f"✅ Success: {success_count} movies")
print(f"❌ Failed: {fail_count} movies")
print(f"📁 Saved to: {output_file}")
print(f"{'='*50}")