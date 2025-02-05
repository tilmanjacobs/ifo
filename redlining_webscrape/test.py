cities = ['A', 'B', 'C', 'D']


for city in cities:
     for doc_no in range(1,9):
        print(f"{city}_{doc_no}")
        

#for city in cities:
#    for doc_no in range(8, 0, -1):
#        city_doc = f"{city}_{doc_no}"
        # Check if base URL exists for this city_doc
        #    if f"base_url_{city_doc}" not in globals():
#            print(f"No base URL found for {city_doc}, skipping...")
#            break    
#        page_no = 137
#    while True:
         #   try:
                #success = download_image(city_doc, page_no)
                #if not success:
          #          break
                #page_no += 1
            #except Exception as e:
                #print(f"Stopped at page {page_no} due to error: {e}")
                #break
cities = cities[1:]
