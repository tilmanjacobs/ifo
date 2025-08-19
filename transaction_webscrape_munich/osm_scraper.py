import osmnx as ox
import pandas as pd
import matplotlib.pyplot as plt

def get_places_by_tag(place_name, tag_key, tag_value):
    """
    Get places (nodes) that match specific tags in a given area.
    
    Args:
        place_name (str): Name of the area to search in (e.g., 'Munich, Germany')
        tag_key (str): OSM tag key (e.g., 'amenity')
        tag_value (str): OSM tag value (e.g., 'restaurant')
    
    Returns:
        GeoDataFrame: Places matching the criteria
    """
    try:
        # Get places that match the tag criteria
        places = ox.geometries_from_place(
            place_name,
            tags={tag_key: tag_value}
        )
        return places
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def get_road_network(place_name):
    """
    Get the road network for a specific area.
    
    Args:
        place_name (str): Name of the area to search in (e.g., 'Munich, Germany')
    
    Returns:
        NetworkX graph: Road network
    """
    try:
        # Get the road network
        G = ox.graph_from_place(place_name, network_type='drive')
        return G
    except Exception as e:
        print(f"Error fetching road network: {e}")
        return None

def plot_places(places, title):
    """
    Plot places on a map.
    
    Args:
        places (GeoDataFrame): Places to plot
        title (str): Title for the plot
    """
    if places is not None and not places.empty:
        ax = places.plot(figsize=(12, 8))
        plt.title(title)
        plt.show()
    else:
        print("No places to plot")

def main():
    # Example 1: Get all restaurants in Munich
    munich_restaurants = get_places_by_tag('Munich, Germany', 'amenity', 'restaurant')
    if munich_restaurants is not None:
        print(f"Found {len(munich_restaurants)} restaurants in Munich")
        # Save to CSV
        munich_restaurants.to_csv('munich_restaurants.csv')
        # Plot the restaurants
        plot_places(munich_restaurants, 'Restaurants in Munich')

    # Example 2: Get all schools in Munich
    munich_schools = get_places_by_tag('Munich, Germany', 'amenity', 'school')
    if munich_schools is not None:
        print(f"Found {len(munich_schools)} schools in Munich")
        # Save to CSV
        munich_schools.to_csv('munich_schools.csv')
        # Plot the schools
        plot_places(munich_schools, 'Schools in Munich')

    # Example 3: Get the road network of Munich
    munich_roads = get_road_network('Munich, Germany')
    if munich_roads is not None:
        print("Successfully fetched Munich road network")
        # Plot the road network
        ox.plot_graph(munich_roads, figsize=(12, 8), node_size=0, edge_linewidth=0.5)
        plt.title('Road Network of Munich')
        plt.show()

if __name__ == "__main__":
    main() 