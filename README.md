# NS Schools Map

Interactive Folium map with:
- Filters (Status, Group) + search
- List view synced to map
- Add by address or click, move, delete + undo
- Phone & email in popups
- CSV download (All + Filtered)

## Run (CSV)
python ns_school_mapper_interactive_v9_2.py --input "ns_schools_map_editable.csv" --output "ns_schools_map_editable.html" --no-cache

## Run (Excel)
python ns_school_mapper_interactive_v9_2.py --input "2024_2025 Elementary Schools.xlsx" --output "ns_schools_map_editable.html" --regeocode-failed --min-delay-seconds 2.0
