import geopandas as gpd
import pandas as pd

# Load the countries GeoJSON
world = gpd.read_file("https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson")

# Extract and sort unique country names
countries_df = pd.DataFrame(sorted(world['name'].unique()), columns=['name'])

print(countries_df)  # Display the first few country names
print(world[world["name"].str.contains("Croatia", case=False)]) # Display the geometry of Croatia
