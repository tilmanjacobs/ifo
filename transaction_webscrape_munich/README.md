# OpenStreetMap Data Scraper

This script allows you to scrape various types of data from OpenStreetMap using the OSMnx library.

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

The script provides several functions to scrape different types of data from OpenStreetMap:

1. `get_places_by_tag(place_name, tag_key, tag_value)`: Get places (nodes) that match specific tags in a given area
   - Example: Get all restaurants in Munich
   ```python
   restaurants = get_places_by_tag('Munich, Germany', 'amenity', 'restaurant')
   ```

2. `get_road_network(place_name)`: Get the road network for a specific area
   - Example: Get the road network of Munich
   ```python
   roads = get_road_network('Munich, Germany')
   ```

3. `plot_places(places, title)`: Plot places on a map
   - Example: Plot restaurants
   ```python
   plot_places(restaurants, 'Restaurants in Munich')
   ```

## Example Data Types

You can scrape various types of data by using different tag combinations. Here are some common examples:

- Restaurants: `amenity=restaurant`
- Schools: `amenity=school`
- Hospitals: `amenity=hospital`
- Parks: `leisure=park`
- Shops: `shop=*`
- Buildings: `building=*`

## Output

The script will:
1. Print the number of items found
2. Save the data to CSV files
3. Display plots of the data on a map

## Customization

You can modify the script to:
- Change the area of interest
- Add different types of data to scrape
- Modify the visualization settings
- Add additional data processing steps 