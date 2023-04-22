import pandas as pd
import numpy as np
import requests
import time
from bs4 import BeautifulSoup
import urllib.parse
from queue import Queue
from threading import Thread



# Dictionary qith query data
query = {
    "place": [
        'Barranquilla, Atlántico', 
        'Medellín, Antioquia', 
        'Melgar, Tolima', 
        'Girardot, Cundinamarca', 
        'Anapoima, Cundinamarca', 
        'Anolaima, Cundinamarca', 
        'La-Mesa, Cundinamarca', 
        'Tocaima, Cundinamarca', 
        'Villa-de-Leyva, Boyacá',
        'Apulo,Cundinamarca'

    ],
    "checkin": '2022-11-11',
    "checkout": '2022-11-14',
    "adults": 3,
    "children": 2,
    "infants": 0,
    "pets": 1,
}


# Selectors for extract data the results page
selectors_general = [
    {
        "name": "price",
        "selector": "span._tyxjp1",
        "default": "",
        "clean": ["\xa0", "COP", "\n"],
    }, 
    {
        "name": "place",
        "selector": "div.t1jojoys.dir.dir-ltr",
        "default": "",
        "clean": ["\n"],        
    },
    {
        "name": "space",
        "selector": "span.t6mzqp7.dir.dir-ltr",
        "default": "",
        "clean": ["\n"],
    },
    {
        "name": "score",
        "selector": "span.r1dxllyb.dir.dir-ltr",
        "default": "0",
        "clean": ["(", ")", "\n"],
    },
    {
        "name": "tag",
        "selector": "a.ln2bl2p.dir.dir-ltr",
        "default": "",
        "clean": [],
    },
    {
        "name": "huespedes",
        "selector": "span.r1dxllyb.dir.dir-ltr",
        "default": "0",
        "clean": ["(", ")", "\n"],
    },
]

# Variable for save the airbnb scraping data
data = Queue ()

# numbef of threads for web scraping
threads_num = 10

def build_links(place='', checkin='', checkout='', adults=0, children=0, infants=0, pets=0):
    url = 'https://www.airbnb.com.co/s/'
    flag = False
    links2 = []

    for q in place:
        if len(place) > 0:
            text = q.split(",")
            city = text[0]
            # query = urllib.parse.quote(str(query))
            city1 = text[1]
            cityglo = city.strip() + '--' + city1.strip() + '--Colombia/homes?tab_id=home'
            url1 = url + cityglo

            format = "%Y-%m-d"
            if len(checkin) == 10:
             #checkin=datetime.datetime.strptime(checkin, format)
                if flag:
                    url1 = url1 + '&checkin=' + checkin
                else:
                    url1 = url1 + '&checkin=' + checkin
                    flag = True
            if len(checkout) == 10:
                #checkout=datetime.datetime.strptime(checkout, format)
                if flag:
                    url1 = url1 + '&checkout=' + checkout
                else:
                    url1 = url1 + '?checkout=' + checkout
                    flag = True

            if adults > 0:
                if flag:
                    url1 = url1 + '&adults=' + str(adults)
                else:
                    url1 = url1 + '?adults=' + str(adults)
                    flag = True

            if children > 0:
                if flag:
                    url1 = url1 + '&children=' + str(children)
                else:
                    url1 = url1 + '?children=' + str(children)
                    flag = True
            if infants > 0:
                if flag:
                    url1 = url1 + '&infants=' + str(infants)
            else:
                url1 = url1 + '?infants=' + str(infants)
                flag = True

            if pets > 0:
                if flag:
                    url1 = url1 + '&pets=' + str(pets)
            else:
                url1 = url1 + '?pets=' + str(pets)
                flag = True
            url2 = url1 + ''
            links2.append(url2)

    return links2



def scrape_data (links):
    # Scraping data from each link and save in global queue

    # Loop for each search link
    for link in links:
        
        
        # Variable for calculate the current results page
        current_page = 0
        
        # Loop for extract data from all results pages
        while True:
            
            # Calculate and add offset to link (pagination)
            offset = 20*current_page 
            link_offset = f"{link}&items_offset={offset}"
            
            # Requests data to the page and check for errors
            res = requests.get (link_offset)
            
            # Detect requests error and try again
            try:
                res.raise_for_status ()
            except:
                time.sleep (60)
                
                # Retry
                try:
                    res = requests.get (link_offset)
                except:
                    print (f"Error scraping page: '{link_offset}', and retry failed. Page skipped\n")
                    
            
            # Send data to bs4
            soup = BeautifulSoup(res.content, 'html.parser')

            # Count number of results in page
            # selector_result = ".gh7uyir.g1xypvzw.g14v8520.dir.dir-ltr >  .dir.dir-ltr"
            selector_result = ".gh7uyir.giajdwt.g14v8520.dir.dir-ltr > .dir.dir-ltr"
            results = soup.select (selector_result)
            
            # End page extraction where no more results (at the last page)
            if not results:
                break
            
            # Show status
            results_num = len(results)
            show_url = link[:60].replace('https://www.airbnb.com.co', '...')
            print (f"Searching in: {show_url}..., Offset: {offset}\n")
            
            # Loop for each result in page for extract data
            for result in results:
                
            
                # Find elements (html tags) in the result and get texts
                result_data = {}
                for selector_obj in selectors_general:
                    
                    # Get element and text who match with the selector
                    elem = result.select_one(selector_obj["selector"])
                    
                    # Validate if the elements exist
                    if not elem:
                        # Save default value and go to the next register
                        result_data[selector_obj["name"]] = (selector_obj["default"])
                        continue
                    
                    # Get text
                    text = elem.text
                    
                    # Clean text if exist clean elemts
                    for clean_elem in selector_obj["clean"]:
                        text = text.replace (clean_elem, "")
                       
                        
                    # Fix specific selectors
                    
                    if selector_obj["name"] == "score":
                        # Only get left part of the score
                        text_parts = text.split()
                        text = text_parts[0]

                        
                    if selector_obj["name"] == "tag":
                        # Get link or the tag
                        text = f'https://www.airbnb.com.co{elem["href"]}'                    
                    
                    # Save text
                    result_data[selector_obj["name"]] = text
                                
                # Get room type
                if 'habitación privada' in result_data["place"].lower():
                    room_type = 'compartido'
                else:
                    room_type = 'privado'
                result_data["room_type"] = room_type


                place = {
                       
                            'Barranquilla, Atlántico', 
                            'Medellín, Antioquia', 
                            'Melgar, Tolima', 
                            'Girardot, Cundinamarca', 
                            'Anapoima, Cundinamarca', 
                            'Anolaima, Cundinamarca', 
                            'La-Mesa, Cundinamarca', 
                            'Tocaima, Cundinamarca', 
                            'Villa-de-Leyva, Boyacá',
                            'Apulo,Cundinamarca'

                    }

                for q in place:
                  if len(place) > 0:
                    text = q.split(",")
                    city = text[0]
                  result_data["city"] = city
           
                    
                # Request data from details page
                r = requests.get(result_data["tag"])
                soup = BeautifulSoup(r.content, 'html.parser')

                n_descripcion_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(3) > div > div._16e70jgn > div > div:nth-of-type(4) > div > div:nth-of-type(2) > div.d1isfkwk.dir.dir-ltr > div > span > span')
                if n_descripcion_elem == None:
                  n_descripcion = ''
                else:
                  n_descripcion = n_descripcion_elem.text
                result_data["n_descripcion"] = n_descripcion
                

                # n_lugares_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(1) > div:nth-of-type(1) > div > div > div > div > section > div._1qdp1ym > div._dm2bj1 > span:nth-of-type(5) > button > span')
                # if n_lugares_elem == None:
                #   n_lugares = ''
                # else:
                #   n_lugares = n_lugares_elem.text
                # result_data["n_lugar"] = n_lugares

                # # Get number of guests
                n_huspedes_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(3) > div > div._16e70jgn > div > div:nth-of-type(1) > div > div > section > div > div > div > div._tqmy57 > ol > li:nth-of-type(1) > span:nth-of-type(1)')
                if n_huspedes_elem == None:
                  n_huspedes = ''
                else:
                  n_huspedes = n_huspedes_elem.text
                result_data["n_huspedes"] = n_huspedes
                
                #Get number of bathdrooms
                n_bathrooms_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(3) > div > div._16e70jgn > div > div:nth-of-type(1) > div > div > section > div > div > div > div._tqmy57 > ol > li:nth-of-type(4) > span:nth-of-type(2)')
                if n_bathrooms_elem == None:
                  n_bathrooms = ''
                else:
                  n_bathrooms = n_bathrooms_elem.text
                result_data["n_bathrooms"] = n_bathrooms
                
                #Get number of beds
                n_beds_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(3) > div > div._16e70jgn > div > div:nth-of-type(1) > div > div > section > div > div > div > div._tqmy57 > ol > li:nth-of-type(3) > span:nth-of-type(2)')
                if n_beds_elem == None:
                  n_beds = ''
                else:
                  n_beds = n_beds_elem.text
                result_data["n_beds"] = n_beds

                #Get number rooms
                n_rooms_elem = soup.select_one('#site-content > div > div:nth-of-type(1) > div:nth-of-type(3) > div > div._16e70jgn > div > div:nth-of-type(1) > div > div > section > div > div > div > div._tqmy57 > ol > li:nth-of-type(2) > span:nth-of-type(2)')
                if n_rooms_elem == None:
                  n_rooms = ''
                else:
                  n_rooms = n_rooms_elem.text
                result_data["n_rooms"] = n_rooms

                # n_comments = soup.find('span','class="'#site-content > div > div:nth-of-type(1) > div:nth-of-type(4) > div > div > div > div:nth-of-type(2) > section > div:nth-of-type(3) > div > div > div:nth-of-type(2) > div > div:nth-of-type(2) > div:nth-of-type(1) > span ')
                n_comments = soup.find('span',attrs={"class":"ll4r2nl dir dir-ltr"}) 
                if n_comments == None:
                  n_comment = ''
                else: 
                  n_comment = n_comments.text
                result_data["n_comment"] = n_comment

                """ EXTRACT MORE DATA HERE"""
                
                # Save current result data in global data
                data.put (result_data)
                
            # Incress page counter
            current_page += 1
                                               
                
# Generate links with function, sending query varuables
links = build_links(
    query["place"],
    query["checkin"],
    query["checkout"],
    query["adults"],
    query["children"],
    query["infants"],
    query["pets"]
)

# Split link in sublists
links_chunks = np.array_split(links, threads_num)

# Create and start threads with sublist of links
threads = []
for link_chunk in links_chunks:
    if (link_chunk.size > 0): 
        thread_obj = Thread (target=scrape_data, args=(link_chunk,))
        thread_obj.start ()
        threads.append (thread_obj)

# loop for wait threads end
while True:
    # Check if any of the threads its alive
    threads_alive = list(filter(lambda thread: thread.is_alive(), threads))
    
    # Wait if any of the threads still running
    if threads_alive:
        time.sleep (1)
        continue
    
    # End wait time
    else:
        break   
    
# End threads
for thread in threads:
    thread.join ()


""" MANAGE DATA HERE """

# Convert queue to list
data_list = list(data.queue)


# Convert data to list
data_list = map(lambda row: list(row.values()),data_list)

# Save data in csv
import csv
with open ("data.csv", "w", newline='', encoding="UTF-8") as file:
    csv_writer = csv.writer(file)

    csv_writer.writerows (data_list)
