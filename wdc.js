(function() {

    /********************************************************************/
    /*  Define what API Calls/Tables are available in this connector    */
    /********************************************************************/

    var tables = {
        "activities": {
            "wdcSchema": "schemas/tableau/activities.json",
            "getData": function(table, accessToken, doneCallback){

                //  Get the strava schema for the activities table
                getSchema("strava","activities")
                    .then( function(schema){

                        //  Define the post-processing function
                        function postProcesser(data){

                            //  Create an array to hold the transformed data
                            var processedData = [];

                            //  Loop through each record
                            data.forEach(function(row, index){

                                //  Init an empty object to hold all this data
                                var record = {};
                                
                                //  Loop through each "column" or property in this row (using strava's schema)
                                schema.responseFields.forEach(function(field){
                                    //  Use the strava schema to determine the path, and save it to the tableau record
                                    //  We need to use the getColumnId function to convert the path (with . symbols) to an acceptable id for Tableau
                                    record[getColumnId(schema.id,field.path)] = resolve(field.path,row);
                                })

                                //  Append
                                processedData.push(record);
                            })

                            //  Done processing data, save to tableau object
                            table.appendRows(processedData);
                            
                            //  All done, notify Tableau
                            doneCallback();
                        }

                        //  Loop through each page of activity data, then run the postProcesser function
                        executeApi(accessToken, schema.baseUrl, 1, [], postProcesser);
                    })
            }
        },
        "gear": {
            "wdcSchema": "schemas/tableau/gear.json",
            "getData": function(table, accessToken, doneCallback){

                //  Get the strava schema for the activities table
                getSchema("strava","gear")
                    .then( function(schema){

                        //  Define the post-processing function
                        function postProcesser(data){

                            //  Create an array to hold the transformed data
                            var processedData = [];

                            data.forEach(function(athlete){

                                //  Loop through each type of gear
                                schema.responseArrays.forEach(function(gearType){

                                    //  Loop through all results
                                    var gearList = athlete[gearType] ? athlete[gearType] : [];
                                    gearList.forEach(function(row){

                                        //  Initialize a record for each gear item
                                        var record = {
                                            "gear_type": gearType
                                        }

                                        //  Loop through each gear property
                                        schema.responseFields.forEach(function(field){
                                            //  Use the gear schema to determine the path, and save it to the tableau record
                                            //  We need to use the getColumnId function to convert the path (with . symbols) to an acceptable id for Tableau
                                            record[getColumnId(schema.id,field.path)] = resolve(field.path,row);
                                        })

                                        //  Save the gear record
                                        processedData.push(record)
                                    })
                                })
                            })

                            
                          
                            //  Done processing data, save to tableau object
                            table.appendRows(processedData);
                            
                            //  All done, notify Tableau
                            doneCallback();
                        }

                        //  Loop through each page of activity data, then run the postProcesser function
                        executeApi(accessToken, schema.baseUrl, 0, [], postProcesser);
                    })
            }
        },
        "athlete": {
            "wdcSchema": "schemas/tableau/athlete.json",
            "getData": function(table, accessToken, doneCallback){

                //  Get the strava schema for the activities table
                getSchema("strava","athlete")
                    .then( function(schema){

                        //  Define the post-processing function
                        function postProcesser(data){

                            //  Initialize a singlerecord for the user
                            var record = {}

                            //  Loop through all results
                            data.forEach(function(row){

                                //  Loop through each gear property
                                schema.responseFields.forEach(function(field){
                                    //  Use the athlete schema to determine the path, and save it to the tableau record
                                    //  We need to use the getColumnId function to convert the path (with . symbols) to an acceptable id for Tableau
                                    record[getColumnId(schema.id,field.path)] = resolve(field.path,row);
                                })

                                //  Done processing data, save to tableau object
                                table.appendRows([record]);
                            })
                            
                            //  All done, notify Tableau
                            doneCallback();
                        }

                        //  Loop through each page of activity data, then run the postProcesser function
                        executeApi(accessToken, schema.baseUrl, 0, [], postProcesser);
                    })
            }
        },
        "activityStreams": {
            "wdcSchema": "schemas/tableau/activityStreams.json",
            "getData": function(table, accessToken, doneCallback){

                //  Get the strava schema for the activities table
                getSchema("strava","activities")
                    .then( function(schema){

                        //  Define the post-processing function
                        function postProcesser(data){

                            //  Create an array to hold the transformed data, and an array to hold promises for each activity's data stream
                            var processedData = [],
                                streamPromises = [];

                            //  Loop through each record
                            data.forEach(function(activity, index){

                                //  Create a promise for each activity
                                var activityStreamPromise = new Promise(function(resolve,reject){

                                    //  Define what stream types we want to query for
                                    var streamTypes = ['distance','altitude','time','latlng','velocity_smooth','heartrate','grade_smooth'];

                                    //  Initialize a baseline record, based on the activity data
                                    var getRecord = function(){
                                        return {
                                            "activity_id": activity.id,
                                            "activity_athlete_id": activity.athlete.id,
                                            "activity_gear_id": activity.gear_id,
                                            "activity_kudos_count": activity.kudos_count,
                                            "activity_elapsed_time": activity.elapsed_time,
                                            "activity_moving_time": activity.moving_time,
                                            "activity_private": activity.private,
                                            "activity_commute": activity.commute,
                                            "activity_start_date": activity.start_date,
                                            "activity_start_date_local": activity.start_date_local,
                                            "activity_utc_offset": activity.utc_offset,
                                            "activity_timezone": activity.timezone,
                                            "activity_type": activity.type,
                                            "activity_trainer": activity.trainer,
                                            "activity_pr_count": activity.pr_count,
                                            "activity_comment_count": activity.comment_count,
                                            "activity_achievement_count": activity.achievement_count,
                                            "activity_name": activity.name,
                                            "activitystream_lat": null,
                                            "activitystream_lng": null,
                                            "activitystream_distance": null,
                                            "activitystream_altitude": null,
                                            "activitystream_time": null,
                                            "activitystream_velocity_smooth": null,
                                            "activitystream_heartrate": null,
                                            "activitystream_grade_smooth": null
                                        }
                                    };

                                    //  Does this activity have a data stream? It won't for manually entered activities
                                    if (activity.manual) {

                                        //  Manually added activities won't have stream data, skip the additional API call
                                        //  Instead fill in some stream fields, using the aggregates from the activity
                                        var manualActivity = getRecord();
                                        manualActivity.activitystream_lat = Array.isArray(activity.start_latlng) ? activity.start_latlng[0] : null;
                                        manualActivity.activitystream_lng = Array.isArray(activity.start_latlng) ? activity.start_latlng[1] : null;
                                        manualActivity.activitystream_distance = activity.distance;
                                        //  No data to fill in for altitude
                                        manualActivity.activitystream_time = activity.moving_time;
                                        manualActivity.activitystream_velocity_smooth = activity.average_speed;
                                        //  No data to fill in for heartrate
                                        //  No data to fill in for grade

                                        //  Resolve the promise with this data point
                                        resolve([manualActivity]);

                                    } else {

                                        //  This activity should have stream data, make an additional API call
                                        $.ajax({ 
                                            "url": "https://www.strava.com/api/v3/activities/" + activity.id + "/streams/" + streamTypes.join(), 
                                            "method": "get",
                                            "headers": {
                                                "Authorization": "Bearer " + accessToken
                                            },
                                            "success": function(streams){

                                                //  Init an empty array to hold all this stream data, and a single object to hold the activity-level data
                                                var data = [];

                                                //  Determine the size of the stream
                                                var streamSize = streams.length ? streams[0].original_size : 0;
                                                if (streamSize===0) {
                                                    //  Somehow we got an empty stream back for this activity
                                                    resolve([getRecord()])
                                                } else {

                                                    //  This stream has some activity data.
                                                    //  Loop through each stream data point
                                                    for (var i=0; i<streamSize; i++){

                                                        //  Create a copy of the activity record
                                                        var streamRecord = getRecord();  // $.extend(record,{});

                                                        //  Append all stream data as columns
                                                        streams.forEach(function(stream){
                                                            //  Special handler for latlng type
                                                            if (stream.type==="latlng"){
                                                                streamRecord["activitystream_lat"] = stream.data[i][0];
                                                                streamRecord["activitystream_lng"] = stream.data[i][1];
                                                            } else {
                                                                streamRecord["activitystream_" + stream.type] = stream.data[i];
                                                            }
                                                        })
        
                                                        //  Append this stream record to the data set
                                                        data.push(streamRecord);
                                                    }

                                                    //  Resolve with all data from this activity stream
                                                    resolve(data)
                                                }
                                            },
                                            "error": function(xhr,statusCode,error){
                                                //  Resolve this activity w/ no stream data
                                                log("No stream data for activity " + activity.id,false);
                                                resolve([getRecord()])
                                            }
                                        })
                                    }
                                })
                                streamPromises.push(activityStreamPromise)
                            })

                            //  Execute API calls for each activity's data stream
                            Promise.all(streamPromises).then(function(data){

                                var allData = [];

                                //  Loop through each activity's stream data, and save to the table
                                data.forEach(function(activityStreamData){
                                    allData = allData.concat(activityStreamData);
                                })

                                //  All done, notify Tableau
                                table.appendRows(allData);
                                doneCallback();
                            }).catch(function(error){
                                //  Handle errors during the query
                                log(error.message,true)
                            })
                        }

                        //  Loop through each page of activity data, then run the postProcesser function
                        executeApi(accessToken, schema.baseUrl, 1, [], postProcesser);
                    })
            }
        },
    }

    /********************************************************************/
    /*  Helper Functions                                                */
    /********************************************************************/

    //  Parse values from a query string
    function parseQueryString(){

        //  Check to see if there's a query string at all
        if (window.location.href.split("?").length > 1) {

            //  There is a query string
            var qs = window.location.href.split("?")[1].split("&");
            var queryParams = {};
            for ( param in qs ){
                var equation = qs[param].split("=");
                queryParams[equation[0]] = equation[1];
            }
            return queryParams
        } else {
            //  No query string
            return {}
        }     
    }

    //  Fetch values from an object, based on a path (for nested properties)
    /*
        path: the path used to find the nested property you want (ex. 'object.subObject.property' or 'array.0.property')
        obj: the object to look inside
    */
    function resolve(path,obj) {
        var i, len;

        for(i = 0,path = path.split('.'), len = path.length; i < len; i++){
            if(!obj || typeof obj !== 'object') return null;
            obj = obj[path[i]];
        }

        if(obj === undefined) return null;
        return obj;
    }

    //  Generate column ids for tableau
    function getColumnId(tableName, path){
        //  Replace all dot notations with underscores, and prepend w/ table name
        return tableName + '_' + path.replace(/\./g,"_");
    }

    //  Logging function
    /*
        message: message to log
        abort: should this event also trigger an error and stop the WDC execution? (true/false)
    */
    function log(text, abort){
        var message = "_Strava WDC_ " + text;
        console.log(message);
        tableau.log(message);
        if (abort){
            tableau.abortWithError(message);
        }
    }

    //  Function to fetch a schema
    /*
        type: strava or tableau
        schemaName: the name of the schema file to fetch (no file extension needed)
    */
    function getSchema(type, schemaName){
        return new Promise(function(resolve,reject){
            $.ajax({ 
                "url": "schemas/" + type + "/" + schemaName + ".json", 
                "dataType": "json",
                "success": function(data){
                    resolve(data)
                },
                "error": function(error){
                    reject({"error":true,"message":"Error fetching " + type + " Schema for " + schemaName})
                }})
        })
    }

    //  Function to make API calls
    /*
        accessToken: access_token required to authenticate each api call
        url: base url of the api call
        currentPage: if you don't need to page through results enter 0, otherwise 1
        callback: function to execute when the API call(s) are finished
    */
    function executeApi(accessToken, url, currentPage, tableData, callback){

        //  Define the default page size
        var pageSize = 100;

        //  Define the url
        var queryString = currentPage==0 ? "" : "?page=" + currentPage + "&per_page=" + pageSize;
        //var queryString = currentPage==0 ? "" : "?page=" + currentPage + "&per_page=" + pageSize + "&after=1571626506";

        //  Define the API call options
        $.ajax({
            "url": url + queryString,
            "method": "get",
            "headers": {
                "Authorization": "Bearer " + accessToken
            },
            "success": function(data){

                //  Append these new records to the original data array
                var fullData = tableData.concat(data);

                //  Do we need to make more API calls for additional pages of data?
                if ((currentPage>0) && (pageSize==data.length)){
                    //  Increment the counter, and make another call
                    executeApi(accessToken, url, currentPage+1, fullData, callback)    
                } else {
                    //  All done, notify Tableau
                    callback(fullData);
                }
            },"error": function(req,status,error){
                var message = "Error during getData phase, using access token:" + auth.accessToken + " - " + error;
                log(message,true);
            }
        })
    }

    //  Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submit").click(authorize);
    });


    /********************************************************************/
    /*  Business Logic                                                  */
    /********************************************************************/

    //  Interaction Phase: The user opened this WDC in Desktop, entered their information, and clicked the submit button.
    //                      Now we need to start the OAuth flow.
    function authorize(){

        //  Get the client id and secret
        var clientId = document.getElementById("clientId").value,
            clientSecret = document.getElementById("clientSecret").value;

        //  Save both these values in the browser's session storage
        sessionStorage.clear();
        sessionStorage.setItem("clientId", clientId);
        sessionStorage.setItem("clientSecret", clientSecret);

        //  Generate the redirect url
        var redirect_url = "https://www.strava.com/oauth/authorize?client_id=" + clientId 
                            + "&scope=activity:read_all,profile:read_all"
                            + "&response_type=code"
                            + "&approval_prompt=auto"
                            + "&redirect_uri=" + window.location.href;

        //  redirect the browser window
        window.location.href = redirect_url;
        return false;
    }

    //  Promise to get a new access token (needed to fetch data)
    function getAccessToken(auth, grantType, initCallback) {

        //  Define the payload to send
        var payload = {
            "client_id": auth.clientId,
            "client_secret": auth.clientSecret,
            "grant_type": grantType
        }

        //  Decide what type of code/token to pass in
        if (grantType=="refresh_token"){
            payload["refresh_token"] = auth.refreshToken;
        } else if (grantType=="authorization_code"){
            payload["code"] = auth.code;
        }

        //  Make the API call
        return $.ajax({
            "url":  "https://www.strava.com/api/v3/oauth/token",
            "method": "POST",
            "data": payload,
            "success": function(data) {

                //  Store the client id/secret, access code, & refresh token in the connectionData
                tableau.connectionData = JSON.stringify({
                    "clientId": auth.clientId,
                    "clientSecret": auth.clientSecret,
                    "code": auth.code,
                    "refreshToken": data.refresh_token
                })

                //  Store the access token & expiry time as the password
                tableau.password = JSON.stringify({
                    "accessToken":data.access_token,
                    "expiry": data.expires_at
                })

                //  We've got everything we need, initialization complete
                initCallback();
                tableau.submit();
            },
            "error": function(xhr,status,error){
                var message = "Error while exchanging a " + grantType + " for an access token - " + error;
                log(message,true);
                reject(xhr)
            }
        })
    }

    /********************************************************************/
    /*  Define the Tableau Web Data Connector                           */
    /********************************************************************/
    
    //  Create a new connector object
    var myConnector = tableau.makeConnector();

    //  Define the initialization Phase
    //  
    //  This function gets called under the following scenarios:
    //      Interactive Phase (Desktop): wait for user to enter details, then trigger tableau.submit
    //      Auth Phase (Server): we have refresh token saved as embeded credentials, need to make an API call to get an access token
    //      Gather Data Phase: (Desktop & Server): We have a valid access token, can start quering for data

    myConnector.init = function(initCallback) {

        //  Specify a custom authentication type, since we're using oauth
        tableau.authType = tableau.authTypeEnum.custom;

        //  Check connectionData for a saved client id/secret, access code, and refresh token
        var connectionData = tableau.connectionData.length ? JSON.parse(tableau.connectionData) : { "clientId":sessionStorage.getItem("clientId"), "clientSecret": sessionStorage.getItem("clientSecret")};

        //  Parse the query string, to look for an access code (from oauth redirect)
        var queryString = parseQueryString();

        //  Check to see if we've already got a saved access token
        var password = tableau.password.length ? JSON.parse(tableau.password) : {};

        //  Is the auth code still valid? Make sure we've got values for both the access token and it's expiration
        //   and check to ensure the expiration datetime isn't in the past.  Also, always assume its expired if running on Tableau Server
        //var accessTokenIsValid = (tableau.authPurpose==="ephemeral") && password.accessToken && password.expiry && ( parseInt(password.expiry)> (new Date().getTime() / 1000));
        var accessTokenIsValid = false;


        //////////////////////////////
        //  OAuth Business Logic    //
        //////////////////////////////

        //  Do we have a saved client id?
        if (connectionData.clientId){

            // Yes, do we have a valid access token?
            if (accessTokenIsValid){

                //  Yes, we have everything needed to fetch data
                initCallback();
                tableau.submit();

                //  Log the event
                var message = "init phase already had a saved access token, using it to fetch data.";
                log(message,false);
            } else {

                //  No access token, but do we have a refresh token?
                if (connectionData.refreshToken) {
                    
                    //  Yes, so make the API call to request a new access token
                    getAccessToken(connectionData, "refresh_token", initCallback)
                        .catch(function(error){
                            
                            //  Error: the refresh token is no longer valid, try using the access code
                            getAccessToken(connectionData, "authorization_code", initCallback)
                                .catch( function(error){
                                    //  Neither the access code nor refresh token would give us a valid access token
                                    var message = "Error - Both access code & refresh token were denied - " + error;
                                    log(message,true);
                                })

                            //  Log the event
                            var message = "tried using a refresh token to get an access code but it failed.  Trying again with the authorization code instead.";
                            log(message,false);
                        });

                    //  Log the event
                    var message = "init phase didn't have an access token, so we'll use an refresh token to request one.";
                    log(message,false);
                } else {

                    //  No refresh token, do we have an access code?
                    if (queryString.code) {
                        
                        //  Yes, add the access code to connection data & make the API call to request a new access token
                        connectionData.code = queryString.code;
                        getAccessToken(connectionData, "authorization_code", initCallback)
                            .catch(function(error){
                                //  The access code was not able to request a refresh token
                                var message = "Error - Could not get a refresh token using the access code - " + error;
                                log(message,true);
                            })

                        //  Log the event
                        var message = "init phase didn't have an access token, so we'll use an access code to request one.";
                        log(message,false);
                    } else {
                        //  No refresh token or access code, invalid client id/secret entered?
                        var error = "We have a client id but no access code for authentication";
                        log(error,true);
                    }
                }
            }
        }

        //  Tell Tableau that the connector is initialized
        //initCallback();
    }

    //  Define the schema
    myConnector.getSchema = function(schemaCallback) {

        //  Init an array to hold table metadata
        var promises = [];

        //  Loop through each table we may pull data for
        for (tablename in tables){
            //  Get the table metadata
            promises.push(getSchema("tableau",tablename))
        }

        //  Execute promises to fetch all schemas, then pass them to the callback function
        Promise.all(promises).then( function(schemas){
            schemaCallback(schemas);
        })
    };

    //  Define how we will get the data
    myConnector.getData = function(table, doneCallback) {

        //  Define an array to hold the resul set data
        var metadata = tables[table.tableInfo.id],
            schema = table.tableInfo.columns;
        
        //  Get the access token from the password property
        var auth = JSON.parse(tableau.password);

        //  Let the metadata define how we are executing API calls to fetch data
        metadata.getData(table, auth.accessToken, doneCallback);

        /*
        //  Define looping criteria
        var pageSize = metadata.pageSize;

        //  Function to make the API call
        function makeApiCall(currentPage) {

            //  Define the url
            var queryString = pageSize==0 ? "" : "?page=" + currentPage + "&per_page=" + pageSize;

            //  Define the API call options
            $.ajax({
                "url": metadata.url + queryString,
                "method": metadata.method,
                "headers": {
                    "Authorization": "Bearer " + auth.accessToken
                },
                "success": function(data){

                    var tableData = [];

                    //  Loop through each result
                    data.forEach( function(row,index){
                        //  Create an object for this record
                        var record = {};
                        //  Loop through the metadata columns, and fetch the data values
                        schema.forEach( function(column){
                            record[column.id] = resolve(column.description, row)
                        })
                        //  Save the row
                        tableData.push(record);
                    })
                    
                    //  Save the first chunk of data returned
                    table.appendRows(tableData);

                    //  Do we need to make more API calls for additional pages of data?
                    if ((pageSize>0) && (pageSize==data.length)){
                        //  Increment the counter, and make another call
                        makeApiCall(currentPage+1)    
                    } else {
                        //  All done, notify Tableau
                        doneCallback();
                    }
                },"error": function(req,status,error){
                    var message = "Error during getData phase, using access token:" + auth.accessToken + " - " + error;
                    log(message,true);
                }
            })
        }

        //  Recursively call the API
        makeApiCall(1);  
        */
    };

    //  Register the connector
    tableau.registerConnector(myConnector);
})();