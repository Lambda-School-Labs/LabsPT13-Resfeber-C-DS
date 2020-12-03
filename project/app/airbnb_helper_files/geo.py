# This is the file that will the code to find all the points that are in a certain 
# radius from the point given

# The bulk of this class was taken from 
# https://github.com/jfein/PyGeoTools/blob/master/geolocation.py
import math

MILES_IN_ONE_KILOMETER = 0.62137
 
class GeoLocation:
    '''
    Class representing a coordinate on a sphere, most likely Earth.
    
    This class is based from the code smaple in this paper:
        http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
        
    The owner of that website, Jan Philip Matuschek, is the full owner of 
    his intellectual property. This class is simply a Python port of his very
    useful Java code. All code written by Jan Philip Matuschek and ported by me 
    (which is all of this class) is owned by Jan Philip Matuschek.
    '''
    DIST_FROM_MILES = 2

    
    MIN_LAT = math.radians(-90)
    MAX_LAT = math.radians(90)
    MIN_LON = math.radians(-180)
    MAX_LON = math.radians(180)
    
    EARTH_RADIUS = 6378.1  # kilometers

    def calc_angular_dist(self):
        """
        Will calculate the angular distance
        of the number in the DIST_FROM_MILES in kilometers
        """
        kil_dis = GeoLocation.from_miles(GeoLocation.DIST_FROM_MILES)
        ans = kil_dis/GeoLocation.EARTH_RADIUS
        return ans

    

    @staticmethod
    def meridian180WithinDistance(boundingCoordinates):
        """
        Will return a boolean if the first longitude is greater than the second
        """  
        theBool = boundingCoordinates[0].rad_lon > boundingCoordinates[1].rad_lon
        return theBool

    @staticmethod
    def from_miles(dist_in_miles):
        """
        This will calculate the distance in kilometers
        """
        k = dist_in_miles / MILES_IN_ONE_KILOMETER
        return k
        
    
    
    @classmethod    
    def from_degrees(cls, deg_lat, deg_lon):
        """
        Builds the GeoLocation class with the degrees
        lat and lon are stored in radian becuase the math needs to be in radians
        
        """
        # Instanciate the class from deg
        rad_lat = math.radians(deg_lat)
        rad_lon = math.radians(deg_lon)
        return GeoLocation(rad_lat, rad_lon, deg_lat, deg_lon)
        
    @classmethod
    def from_radians(cls, rad_lat, rad_lon):
        """
        Instanciating a class from radians 
        """
        # Instanciate the class from radians
        deg_lat = math.degrees(rad_lat)
        deg_lon = math.degrees(rad_lon)
        return GeoLocation(rad_lat, rad_lon, deg_lat, deg_lon)
    
    
    def __init__(
            self,
            rad_lat,
            rad_lon,
            deg_lat,
            deg_lon
            
    ):
        self.rad_lat = float(rad_lat)
        self.rad_lon = float(rad_lon)
        self.deg_lat = float(deg_lat)
        self.deg_lon = float(deg_lon)
        self._check_bounds()
        self.angular_dist = self.calc_angular_dist()
        self.dist_kilo = GeoLocation.from_miles(GeoLocation.DIST_FROM_MILES)
        
    def __str__(self):
        degree_sign= u'\N{DEGREE SIGN}'
        return ("({0:.4f}deg, {1:.4f}deg) = ({2:.6f}rad, {3:.6f}rad)").format(
            self.deg_lat, self.deg_lon, self.rad_lat, self.rad_lon)
        
    def _check_bounds(self):
        if (self.rad_lat < GeoLocation.MIN_LAT 
                or self.rad_lat > GeoLocation.MAX_LAT 
                or self.rad_lon < GeoLocation.MIN_LON 
                or self.rad_lon > GeoLocation.MAX_LON):
            raise Exception("Illegal arguments")
            
    def distance_to(self, other, radius=EARTH_RADIUS):
        '''
        Computes the great circle distance between this GeoLocation instance
        and the other.
        '''
        return radius * math.acos(
                math.sin(self.rad_lat) * math.sin(other.rad_lat) +
                math.cos(self.rad_lat) * 
                math.cos(other.rad_lat) * 
                math.cos(self.rad_lon - other.rad_lon)
            )
            
    def bounding_locations(self, distance, radius=EARTH_RADIUS):
        '''
        Computes the bounding coordinates of all points on the surface
        of a sphere that has a great circle distance to the point represented
        by this GeoLocation instance that is less or equal to the distance argument.
        
        Param:
            distance - the distance from the point represented by this GeoLocation
                       instance. Must be measured in the same unit as the radius
                       argument (which is kilometers by default)
            
            radius   - the radius of the sphere. defaults to Earth's radius.
            
        Returns a list of two GeoLoations - the SW corner and the NE corner - that
        represents the bounding box.
        '''
        
        if radius < 0 or distance < 0:
            raise Exception("Illegal arguments")
            
        # angular distance in radians on a great circle
        rad_dist = distance / radius
        
        min_lat = self.rad_lat - rad_dist
        max_lat = self.rad_lat + rad_dist
        
        if min_lat > GeoLocation.MIN_LAT and max_lat < GeoLocation.MAX_LAT:
            delta_lon = math.asin(math.sin(rad_dist) / math.cos(self.rad_lat))
            
            min_lon = self.rad_lon - delta_lon
            if min_lon < GeoLocation.MIN_LON:
                min_lon += 2 * math.pi
                
            max_lon = self.rad_lon + delta_lon
            if max_lon > GeoLocation.MAX_LON:
                max_lon -= 2 * math.pi
        # a pole is within the distance
        else:
            min_lat = max(min_lat, GeoLocation.MIN_LAT)
            max_lat = min(max_lat, GeoLocation.MAX_LAT)
            min_lon = GeoLocation.MIN_LON
            max_lon = GeoLocation.MAX_LON
        
        return [ GeoLocation.from_radians(min_lat, min_lon) , 
            GeoLocation.from_radians(max_lat, max_lon) ]


            
if __name__ == '__main__':
    # Test degree to radian conversion
    loc1 = GeoLocation.from_degrees(26.062951, -80.238853)
    loc2 = GeoLocation.from_radians(loc1.rad_lat, loc1.rad_lon)
    assert (loc1.rad_lat == loc2.rad_lat and loc1.rad_lon == loc2.rad_lon 
        and loc1.deg_lat == loc2.deg_lat and loc1.deg_lon == loc2.deg_lon)
    
    # Test distance between two locations
    loc1 = GeoLocation.from_degrees(26.062951, -80.238853)
    loc2 = GeoLocation.from_degrees(26.060484,-80.207268)
    assert loc1.distance_to(loc2) == loc2.distance_to(loc1)
    
    # Test bounding box
    loc = GeoLocation.from_degrees(26.062951, -80.238853)
    distance = 1  # 1 kilometer
    SW_loc, NE_loc = loc.bounding_locations(distance)
    print (loc.distance_to(SW_loc))
    print (loc.distance_to(NE_loc))
