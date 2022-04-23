#DOCKER BASE
FROM openjdk:11

#STATIC
ENV OTP_JAR=https://repo1.maven.org/maven2/org/opentripplanner/otp/1.5.0/otp-1.5.0-shaded.jar
ENV BASE_PATH=otp

#DYNAMIC
ENV CITY=atlanta
ENV GTFS_URL=https://www.itsmarta.com/google_transit_feed/google_transit.zip
ENV STATE_PBF=http://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf
ENV BOUNDING_BOX=-84.617195,33.521004,-84.12611,34.010136

#HELPERS
ENV ROUTER_PATH=$BASE_PATH/graphs/$CITY
RUN mkdir -p $ROUTER_PATH

#GET WGET + osmctools
RUN apt update && apt install -y wget osmctools

#DOWNLOAD data
RUN wget $GTFS_URL -O $ROUTER_PATH/gtfs.zip
RUN wget $STATE_PBF -O $ROUTER_PATH/base.pbf

#TRIM and clean up OSM data
RUN osmconvert $ROUTER_PATH/base.pbf -b=$BOUNDING_BOX --complete-ways -o=$ROUTER_PATH/trimmed.pbf && rm $ROUTER_PATH/base.pbf

#DOWNLOAD OTP
RUN wget $OTP_JAR -O otp.jar

# BUILD OTP GRAPH
RUN java -Xmx4G -jar ./otp.jar --build $ROUTER_PATH

ENV CITY=dallas
ENV GTFS_URL=https://transitfeeds.com/p/dart/26/20220322/download
ENV STATE_PBF=http://download.geofabrik.de/north-america/us/texas-latest.osm.pbf
ENV BOUNDING_BOX=-97.090408,32.557906,-96.556455,33.125973

#HELPERS
ENV ROUTER_PATH=$BASE_PATH/graphs/$CITY
RUN mkdir -p $ROUTER_PATH

#DOWNLOAD data
RUN wget $GTFS_URL -O $ROUTER_PATH/gtfs.zip
RUN wget $STATE_PBF -O $ROUTER_PATH/base.pbf

#TRIM and clean up OSM data
RUN osmconvert $ROUTER_PATH/base.pbf -b=$BOUNDING_BOX --complete-ways -o=$ROUTER_PATH/trimmed.pbf && rm $ROUTER_PATH/base.pbf

# BUILD OTP GRAPH
RUN java -Xmx4G -jar ./otp.jar --build $ROUTER_PATH

ENV CITY=chicago
ENV GTFS_URL_CTA=http://www.transitchicago.com/downloads/sch_data/google_transit.zip
ENV GTFS_URL_PACE=https://www.pacebus.com/sites/default/files/2022-04/GTFS.zip
ENV STATE_PBF=http://download.geofabrik.de/north-america/us/illinois-latest.osm.pbf
ENV BOUNDING_BOX=-87.946499,41.643549,-87.521857,42.0841

#HELPERS
ENV ROUTER_PATH=$BASE_PATH/graphs/$CITY
RUN mkdir -p $ROUTER_PATH

#DOWNLOAD data
RUN wget $GTFS_URL_CTA -O $ROUTER_PATH/gtfs_cta.zip
RUN wget $GTFS_URL_PACE -O $ROUTER_PATH/gtfs_pace.zip
RUN wget $STATE_PBF -O $ROUTER_PATH/base.pbf

#TRIM and clean up OSM data
RUN osmconvert $ROUTER_PATH/base.pbf -b=$BOUNDING_BOX --complete-ways -o=$ROUTER_PATH/trimmed.pbf && rm $ROUTER_PATH/base.pbf

# BUILD OTP GRAPH
RUN java -Xmx4G -jar ./otp.jar --build $ROUTER_PATH

ENV CITY=los_angeles
ENV GTFS_URL_dot=https://ladotbus.com/gtfs
ENV GTFS_URL_go=http://data.trilliumtransit.com/gtfs/dpwlacounty-ca-us/dpwlacounty-ca-us.zip
ENV GTFS_URL_metro_bus=https://gitlab.com/LACMTA/gtfs_bus/raw/master/gtfs_bus.zip
ENV GTFS_URL_metro_rail=https://gitlab.com/LACMTA/gtfs_rail/raw/master/gtfs_rail.zip
ENV GTFS_URL_metrolink=https://www.metrolinktrains.com/globalassets/about/gtfs/gtfs.zip
ENV STATE_PBF=http://download.geofabrik.de/north-america/us/california/socal-latest.osm.pbf
ENV BOUNDING_BOX=-118.664435,33.704742,-117.6829,34.328

#HELPERS
ENV ROUTER_PATH=$BASE_PATH/graphs/$CITY
RUN mkdir -p $ROUTER_PATH

#DOWNLOAD data
RUN wget $GTFS_URL_dot -O $ROUTER_PATH/gtfs_dot.zip
RUN wget $GTFS_URL_go -O $ROUTER_PATH/gtfs_go.zip
RUN wget $GTFS_URL_metro_bus -O $ROUTER_PATH/gtfs_metro_bus.zip
RUN wget $GTFS_URL_metro_rail -O $ROUTER_PATH/gtfs_metro_rail.zip
RUN wget $GTFS_URL_metrolink -O $ROUTER_PATH/gtfs_metrolink.zip
RUN wget $STATE_PBF -O $ROUTER_PATH/base.pbf

#TRIM and clean up OSM data
RUN osmconvert $ROUTER_PATH/base.pbf -b=$BOUNDING_BOX --complete-ways -o=$ROUTER_PATH/trimmed.pbf && rm $ROUTER_PATH/base.pbf

# BUILD OTP GRAPH
RUN java -Xmx4G -jar ./otp.jar --build $ROUTER_PATH

ENV CITY=new_york
ENV GTFS_URL_mta_bronx=http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip
ENV GTFS_URL_mta_brklyn=http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip
ENV GTFS_URL_mta_mnhttn=http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip
ENV GTFS_URL_mta_queens=http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip
ENV GTFS_URL_mta_SI=http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip
ENV GTFS_URL_bus_cmpny=http://web.mta.info/developers/data/busco/google_transit.zip
ENV GTFS_URL_subway=http://web.mta.info/developers/data/nyct/subway/google_transit.zip
ENV GTFS_URL_subway_supp=http://web.mta.info/developers/files/google_transit_supplemented.zip
ENV STATE_PBF=http://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf
ENV BOUNDING_BOX=-74.25575705,40.49583814,-73.69960452,40.91516693

#HELPERS
ENV ROUTER_PATH=$BASE_PATH/graphs/$CITY
RUN mkdir -p $ROUTER_PATH

#DOWNLOAD data
RUN wget $GTFS_URL_mta_bronx -O $ROUTER_PATH/gtfs_mta_bronx.zip
RUN wget $GTFS_URL_mta_brklyn -O $ROUTER_PATH/gtfs_mta_brklyn.zip
RUN wget $GTFS_URL_mta_mnhttn -O $ROUTER_PATH/gtfs_mta_mnhttn.zip
RUN wget $GTFS_URL_mta_queens -O $ROUTER_PATH/gtfs_mta_queens.zip
RUN wget $GTFS_URL_mta_SI -O $ROUTER_PATH/gtfs_mta_SI.zip
RUN wget $GTFS_URL_bus_cmpny -O $ROUTER_PATH/gtfs_bus_cmpny.zip
RUN wget $GTFS_URL_subway -O $ROUTER_PATH/gtfs_subway.zip
RUN wget $GTFS_URL_subway_supp -O $ROUTER_PATH/gtfs_subway_supp.zip
RUN wget $STATE_PBF -O $ROUTER_PATH/base.pbf

#TRIM and clean up OSM data
RUN osmconvert $ROUTER_PATH/base.pbf -b=$BOUNDING_BOX --complete-ways -o=$ROUTER_PATH/trimmed.pbf && rm $ROUTER_PATH/base.pbf

# BUILD OTP GRAPH
RUN java -Xmx4G -jar ./otp.jar --build $ROUTER_PATH

#SERVE
CMD java -Xmx5G -jar ./otp.jar --basePath otp/ --server --port 8062 --securePort 8802 \
--router atlanta --router dallas --router new_york --router los_angeles --router chicago

EXPOSE 8062
EXPOSE 8802