# Building the Open Trip Planner server

We use [Open Trip Planner](http://docs.opentripplanner.org/en/v1.5.0/) (OTP) server which is used to generate isochrones (catchment areas) for all points of interest used in the analysis. In case you need to build just this component, follow the steps below.

## Pre-packaged version

We provide a pre-packaged version of the OTP server which uses pre-built graphs that we generated in the process. To build it run the following command.

`docker build -t otp-server-packaged:cse6242 - < OTP-packaged.Dockerfile`

## Complete build

Alternatively, we provide a DockerFile that includes all the instructions required to:
 - install the OTP server
 - download General Transit Feed Specification (GTFS) data feeds used in the analysis
 - build the OTP Graphs using GTFS data and corresponding street networks downloaded from OSM
 - serve the OTP server on the 8062 port.

Note that building this Docker image may take up to 1 hour. To build it run the following command.

`docker build -t otp-server-complete:cse6242 - < OTP-full.Dockerfile`

In case you use this option, note that the GTFS feeds obtained may cover different dates than the pre-packaged versions, and you may need to adjust the date used in the backend configuration (see backend repository for details).

## Atlanta-only build

You can also run a complete build but for Atlanta city only. 

To build it run the following command.

`docker build -t otp-server-atlanta:cse6242 - < atlanta.Dockerfile`

In case you use this option, note that the GTFS feeds obtained may cover different dates than the pre-packaged versions, and you may need to adjust the date used in the backend configuration (see backend repository for details).