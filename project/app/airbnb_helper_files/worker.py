# This is the file that will be used to have the code that is used to work with the 
# postgres database either local or remote


from dotenv import load_dotenv # these are the settings for the connection to database
from psycopg2 import sql
import psycopg2
import os

from app.airbnb_helper_files.geo import GeoLocation
from joblib import load
import pandas as pd
import warnings
import sys

# The multiplier is used to expand the search query radius for the second search
MULTIPLIER = 4 





path_to_model = os.path.join(os.path.dirname(__file__),"..", "models", "gradient_boost_model.joblib")

with warnings.catch_warnings():
      warnings.simplefilter("ignore", category=UserWarning)
      gradient_boost_model = load(path_to_model)


def create_staging(cursor, ):
    cursor.execute(
        """
            DROP TABLE IF EXISTS airb;
             CREATE TABLE airb (
            id      integer PRIMARY KEY,
            lat     numeric (8, 6),
            lon     numeric (8, 6),
            room_type  VARCHAR (255),
            price       integer,
            nin_nights integer
            
        )
        """
    )

def get_avg(tuple_list):
    """
    This function will get the average of the tuplelist that is passed in
    to it. 
    """
    the_val = 0
    # getting the avg of the tuple by doing a loop
    for tup in tuple_list:
        the_val += tup[0] # getting the price out of the tuple 
    ans = the_val/len(tuple_list)
    return ans

def send_csv_psql(connection, csvfile, tableName):
    """
    This is a function that will fill in the data into the postresql database
    """
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    thefile = open(csvfile, 'r')
    table = tableName
    with connection.cursor() as cursor:
        cursor.execute("truncate " + table)
        cursor.copy_expert(sql=sql % table, file=thefile)
    
    cursor.close()

    return connection.commit()


    
def room_to_num(room:str):
    """
    This is the function that will make a numerical representation of the 
    room_type.  This is used to make it so that the room_type
    feature can be used in the model for predicting.
    """
  # the string is checked
    if room == "Private room":
        return 0
    if room == "Entire home/apt":
        return 1
    if room == "Hotel room":
        return 2
    if room == "Shared room":
        return 3

	

def query(bounding_coord, loc :GeoLocation, connection, num_nights, room_type, ang_dist):
    """
    This is the function that will make the query string to look at the database.  This query string is taken from 
    the site 
    """
    # Most of this code is taken or inspired by 
    # http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#:~:text=Moving%20along%20a%20circle%20of%20latitude%20means%20moving%20along%20a,cos(lat))%20%3D%200.9039.
    

    # building the part of the query string dealing with when the location is pole and 180 meridian
    boolVal = "AND"
    if GeoLocation.meridian180WithinDistance(bounding_coord):
        boolVal = "OR"
    sql_lon =  sql.SQL("lon >= {lon_coord_1} {theBool} lon <={lon_coord_2}").format(
        lon_coord_1=sql.Literal(bounding_coord[0].rad_lon),
        theBool=sql.SQL(boolVal),
        lon_coord_2=sql.Literal(bounding_coord[1].rad_lon)
    )
    sql_lon_str = sql_lon.as_string(connection) # making this part of the sql as a string


    # the math function below is used in the query to find those airbnb locations that are with the radius specified
    #"acos(sin(?) * sin(Lat) + cos(?) * cos(Lat) * cos(Lon - ?)) <= ?");
    # This is the portion of the query where using math to select those airbnbs that are withing the radius (loc.radius)
    math_sql = sql.SQL("acos(sin({loc_lat_rad}) * sin(lat) + cos({loc_lat_rad}) * cos(lat) * cos(lon - ({loc_lon_rad}))) <= {dist_divide_radius}").format(
        loc_lat_rad=sql.SQL(str(loc.rad_lat)),
        loc_lon_rad=sql.SQL(str(loc.rad_lon)),
        dist_divide_radius=sql.SQL(str(ang_dist)) # This is the angular distance
    )
    math_str = math_sql.as_string(connection) # turning this portion in to a string that will be added to main query

    sql_ob1 = sql.SQL("SELECT price FROM airbnb WHERE lat >= {lat_coord_1} AND lat <= {lat_coord_2} AND {lon_val} AND {math_val} AND room_type = {room_type} AND num_nights <= {nights}").format(
            lat_coord_1 = sql.Literal(bounding_coord[0].rad_lat), lat_coord_2= sql.Literal(bounding_coord[1].rad_lat),
            theBool = sql.SQL(boolVal),lon_val=sql.SQL(sql_lon_str),
            math_val=sql.SQL(math_str),
            room_type=sql.Literal(room_type),
            nights=sql.Literal(num_nights)
            )
    return sql_ob1


def return_avg_price(lat, lon, room_type, num_nights, conn):
    """
    This function is to find the price average for those items that it pulls 
    from the query.

    conn: the database connection

    """
    
    
   
    
    # counter will be used to make a change to the distance to look will start at 2 miles and the go to 5 
    # if still no results will then use the model
    the_counter = 1
    tuple_list = []
    
    # getting the cursor
    cur = conn.cursor()
    # getting the geolacation and the bounding box
    # Instaciating the GeoLocation class
    geoClass = GeoLocation.from_degrees(deg_lat=lat, deg_lon= lon)
    # getting the bound coord
    bounding = geoClass.bounding_locations(geoClass.dist_kilo, geoClass.EARTH_RADIUS)

    
    # making it so that the radius can grow to 5 once
    while len(tuple_list) < 1 and the_counter <= 2:
        ang_dist = geoClass.angular_dist
        dist = geoClass.dist_kilo
        if the_counter == 2:
            # will increase the radius of the search distance
            ang_dist = geoClass.angular_dist * MULTIPLIER
            dist = dist * MULTIPLIER
            
        bounding = geoClass.bounding_locations(dist)    

        print(f"Info:  The counter number is {the_counter}", file=sys.stderr)  # printing a info statement
        # calling the query function
        sql_obj = query(bounding_coord=bounding, loc=geoClass, connection=conn, 
                        num_nights=num_nights, room_type=room_type, ang_dist=ang_dist)
        # fetching the results
        cur.execute(sql_obj)
        # getting the info
        tuple_list = cur.fetchall()
        
        the_counter +=1 # incrementing the counter



    # closing the connections
    cur.close()
    conn.close()

    
    if len(tuple_list) >= 1: # this will mean that there is a value that is in the tuplelist
        # calling to get the average price 
        price = get_avg(tuple_list=tuple_list)

    # using the model to find the price
    else:
        # making the input into a dataframe to use in the model to predic
        # room_type being changed to a numerical value to use in the model
        print(f"Info:  Using the model to get airbnb price." ,file=sys.stderr)
        room_type = room_to_num(room_type)
        price = gradient_boost_model.predict(pd.DataFrame({"lat":lat, "lon": lon, "room_type": room_type, "num_nights":1},
                                                    index=[0]))[0] # used to get the value out of the prediction array
        
    return round(price, ndigits=2)


if __name__ == "__main__":


    
    
    
    price = return_avg_price(lat=37.3488827, lon= -108.5859265, room_type="Entire home/apt", num_nights=1, conn=connection)

    print(f"The price is the following: {price}")