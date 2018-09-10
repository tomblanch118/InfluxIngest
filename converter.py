import json
from influxdb import InfluxDBClient
from datetime import datetime
import sys
import math
import os.path
#grafana localhost:3000
#sudo service grafana-server start




#date/time format use by influx
timeOutputFormat = '%Y-%m-%dT%H:%M:%SZ'
pushToDB = True#False
boundsCheck = True
batchSize = 1000

def testValue(value, expectedFormat,minVal,maxVal):
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

    if (castValue >= minVal and castValue <= maxVal) or (not boundsCheck):
        return True,castValue

    #print(str(minVal)+" > " + str(castValue)+" > "+str(maxVal))    
    return False,value

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

    if not pushToDB:
        print("---=== DATA WILL NOT BE PUSHED TO DATABASE ===---") 

    user = ''
    password = ''
    database = ''

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


    #TODO: turn into influx line format?
    #TODO: check node address but dont add maybe specify tag/field/ignore
    #TODO: maybe add a processing function name?
        #get the date/time format and expected number of fields from the format file 
    timeFormat = dataFormat.get("datetimeFormat")
    requiredNumFields = dataFormat.get("numberOfFields")
    
    if "boundsCheck" in dataFormat:
        if not dataFormat.get("boundsCheck"):
            boundsCheck = False
            print("Bounds checking off")

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

    for line in tmpFile:

        components = line.split(",")

        #less than the required number of components suggests something went wrong and to distrust/skip the line
        numComponents = len(components)
        if numComponents != requiredNumFields:
            continue 

        #if we can't parse the date/time or it isnt in range we have to skip this line as it is useless without a timestamp
        try:
            timedate = components[0]
            timedate = datetime.strptime(timedate, timeFormat)

            if timedate <  timeMin or timedate > timeMax:
                raise ValueError()

        except ValueError:
            #print('invalid datetime:'+str(timedate))
            continue

        #TODO: get this by name
        node = components[1]

        #convert the date time into the correct format for influx
        timedate = datetime.strftime(timedate,timeOutputFormat)

        index = 0
        for component in components:

            #check data in field against dataFormat spec unless we have specified to ignore
            if dataFormat.get(index)['ignore'] == 'false':

                dType = dataFormat.get(index)['dataType']
                dMin= dataFormat.get(index)['min']
                dMax = dataFormat.get(index)['max']
                componentValid,castValue = testValue(component,dType,dMin,dMax )

                #if the data in the field is valid construct the data point
                if componentValid== True:  
                    fieldName = dataFormat.get(index)['name']

                    datapoint = {}
                    datapoint['measurement'] = dataFormat.get("measurement_name")
                    datapoint['fields'] = {}
                    datapoint['fields'][fieldName] = castValue 
                    datapoint['time'] = timedate 
                    datapoint['tags'] = {}
                    datapoint['tags']['node'] = node
                    points.append(datapoint)

                #TODO:proper debug output
                #else:
                #    sys.stderr.write('Error converting: '+component+"\n")
            index = index + 1

        #dont need this
        #print(json.dumps(points, indent=4, separators=(',',':')))
        #success = client.write_points(points)
        #print(success)
        count = count + 1

        if count > batchSize:
            count=0

            linesProc = linesProc + batchSize

            sys.stdout.write(str((100.0/1143184.0)*linesProc)+"\n")
            sys.stdout.flush()

            if pushToDB:
                client.write_points(points)
            else:
                print(points)
                sys.exit()
            points = []

    #catch any points that weren't written yet
    if pushToDB:
        client.write_points(points,batch_size=1000)

    end = datetime.now()
    diff = (end-start).total_seconds()
    print("Done\nElapsed time: "+str(diff)+" seconds")

