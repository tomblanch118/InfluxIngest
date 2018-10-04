import json
from influxdb import InfluxDBClient
from pytz import UTC
from datetime import datetime
from datetime import timedelta
import sys
import math
import os.path
#grafana localhost:3000
#sudo service grafana-server start




#date/time format use by influx
timeOutputFormat = '%Y-%m-%dT%H:%M:%SZ'
pushToDB = True#False
#boundsCheck = True
batchSize = 1000
delimiter = ","

def coerceValue(value, expectedFormat):
    castValue = ""
    try:
        if expectedFormat == 'int':
            castValue = int(value)
        elif expectedFormat == 'float':
            castValue = float(value)

            #influx doesnt support nan currently, which is shit
            if math.isnan(castValue):
                raise ValueError()
    except ValueError:
        return False,value
        
    return True,castValue


if __name__ == "__main__":


    numArguments = len(sys.argv)

    if "-T" in sys.argv:
        print("Running as test, no data pushed to DB")
        pushToDB = False
        batchSize = 1

    if numArguments < 3:
        print("Must specify filename and configuration file\npython converter.py <fileToProcess.txt> <configFile.fmt>")
        sys.exit()


    inputFilename = sys.argv[1] 
    configFilename = sys.argv[2]
    if not os.path.isfile(inputFilename) or not os.path.isfile(configFilename):
        print("Input file doesn't exist or config file doesn't exist")
        sys.exit()

    tmpFile = open(inputFilename)

    user = None
    password = None
    database = None

    if not os.path.isfile("creds"):
        print("No influxdb credentials found")
        sys.exit()
    else:
        credentials = open("creds")
        
        for line in credentials:
            lineParts = line.strip().split('=')

            lineKey = lineParts[0]
            lineValue = lineParts[1]

            if lineKey == "user":
                user = lineValue
            elif lineKey == "password":
                password = lineValue
            elif lineKey == "database":
                database = lineValue

    if user == None or password == None or database == None:
        print("Missing user, password or database from creds file")
        sys.exit()

    
    if not pushToDB:
        print("---=== DATA WILL NOT BE PUSHED TO DATABASE ===---") 


    client = InfluxDBClient('localhost', 8086, user, password, database)
    print(client)
    start = datetime.now()

    count = 0

    points = []


    inf = open(configFilename)
    try:
        dataFormat = eval(inf.read())
    except SyntaxError:
        print("Failed to parse config file")
        sys.exit()


    #TODO: check node address but dont add maybe specify tag/field/ignore
    #TODO: maybe add a processing function name?
    timeFormat = dataFormat.get("datetimeFormat")
    requiredNumFields = dataFormat.get("numberOfFields")
    
    """if "boundsCheck" in dataFormat:
        if not dataFormat.get("boundsCheck"):
            boundsCheck = False
            print("Bounds checking off")
    """

    try:
        timeMin = datetime.strptime(dataFormat.get(0)['min'],timeFormat)
        timeMax = datetime.strptime(dataFormat.get(0)['max'],timeFormat)
        #print(timeMin)
        #print(timeMax)
    except ValueError:
        print('Invalid min or max datetime specified')
        sys.exit()




    if requiredNumFields < 1:
        print("At least one field must be specified")
        sys.exit()

    if not "measurement_name" in dataFormat:
        print("Missing measurement name")
        sys.exit()

    linesProc = 0


    EPOCH = UTC.localize(datetime.utcfromtimestamp(0))

    #Iterate through the datafile
    for line in tmpFile:

        #split line into component parts
        components = line.split(delimiter)

        #less than the required number of components suggests something went wrong and to distrust/skip the line
        numComponents = len(components)
        if numComponents != requiredNumFields:
            continue 

        #if we can't parse the date/time or it isnt in range we have to skip this line as it is useless without a timestamp
        try:
            timedate = components[0]
            timedate = datetime.strptime(timedate, timeFormat)

            #DST adjust
            #timedate = timedate - timedelta(hours=1)
            if timedate <  timeMin or timedate > timeMax:
                raise ValueError()

        except ValueError:
            continue

        
        #TODO: Honestly not sure how this will work across a summer time boundary?
        #Localise the timestamp so that we can perform the necessary conversions
        tmpTime = UTC.localize(timedate)

        
        #convert the date time into the correct format for influx
        #timedate = datetime.strftime(timedate,timeOutputFormat)

        #Field index
        index = 0

        #Construct the start of the influx line format record
        lfmt = "" + dataFormat.get("measurement_name")
        #lfmt = lfmt + ",node="+node+" "

        #Build a list of fields to be joined to the line format strnig
        fieldsList =[]
        tagList = []

        for component in components:

            #check data in field against dataFormat spec unless we have specified to ignore
            if dataFormat.get(index)['ignore'] == 'false':
    

                #TODO: this is horrible 
                #TODO: check the specification first before doing this then we
                #can get rid of a bunch of repeated checks
                componentType = dataFormat.get(index)['componentType']
                
                if componentType != "tag" and componentType != "field":
                    print("Invalid component type in spec")
                    sys.exit()
                
                doBoundCheck = dataFormat.get(index)['boundCheck']
            
                if doBoundCheck != "true" and doBoundCheck != "false":
                    print("invalid bound check rule: "+ str(doBoundCheck) )
                    sys.exit()


                #Get the type, min and max information from the dataFormat structure
                #and check that it is valid
                dType = dataFormat.get(index)['dataType']
                dMin= dataFormat.get(index)['min']
                dMax = dataFormat.get(index)['max']
                #componentValid,castValue = testValue(component,dType,dMin,dMax )

		coercedSuccess,coercedValue = coerceValue(component,dType)

    		inBounds = (coercedValue >= dMin and coercedValue<= dMax)


                #print(str(coercedSuccess) + ", " + str(inBounds) + ", " + str(doBoundCheck) + ", "+str(coercedValue) )
                #if the data in the field is valid construct the data point
                if( coercedSuccess == True  and  (inBounds == True or doBoundCheck == "false" ) ):  

                    #build the field record from the field name, the data and the type
                    tmp = dataFormat.get(index)['name']+"="
                   
                    if componentType == "field":
                        dType = dataFormat.get(index)['dataType']

                        if dType == "int":
                            tmp = tmp + str(coercedValue) + "i"
                        elif dType == "float":
                            tmp = tmp +str(coercedValue)
                        else:
                            tmp = "\""+ str(coercedValue)+"\""
                        

                        fieldsList.append(tmp)

                    else:
                        #TODO: What should we do it there are no tags?
                        tmp = tmp + str(castValue)
                        tagList.append(tmp)
    
            index = index + 1
        if len(tagList) > 0:
            lfmt = lfmt + "," +",".join(tagList) + " "
        else:
            lfmt = lfmt + " "

        #join all of the fields together as a comma separated string
        lfmt = lfmt + ",".join(fieldsList)

        #Convert the date/time into a nanosecond utc timestamp
        ns = (tmpTime - EPOCH).total_seconds() * 1e9
        lfmt = lfmt + " " + str(int(ns))
        
        #Append the whole influx line format record to the list of records still to be sent to influxdb
        points.append(lfmt)
        count = count + 1

        #Perform batching manually to allow some measure of progress analysis
        if count > batchSize:
            count=0

            linesProc = linesProc + batchSize

            #TODO: actually read the number of lines in file, have to take into account the skipped lines!
            #Provide estimate of progress
            sys.stdout.write(str((100.0/1143184.0)*linesProc)+"\n")
            sys.stdout.flush()

            #If this is not a test run, push the list of influx
            if pushToDB:
                client.write_points(points, protocol="line")
                #sys.exit()
            else:
                print(points)
                sys.exit()
            points = []

    #catch any points that weren't written yet
    if pushToDB:
        client.write_points(points,protocol="line",batch_size=1000)

    end = datetime.now()
    diff = (end-start).total_seconds()
    print("Done\nElapsed time: "+str(diff)+" seconds")

