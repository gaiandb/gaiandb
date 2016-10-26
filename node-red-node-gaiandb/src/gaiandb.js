/**
 * Copyright 2016 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
    "use strict";
	var mustache = require("mustache");
    var path = require("path");

    // releases a connection and handles any error encountered
    function releaseConnection(node, connectionObject){
        node.gaianConfig.jdbc.release(connectionObject, function(err) {
            if (err) {
                node.error(err.message);
            }
        });
    } 
    
    // This function processes a SQL statement, is used by all three gaian nodes, providing
    // common connection handling and error processing. The "type" should be "Query" or "Update" 
    // the resultAction is a function provided to handle a successful query result.
    function processSQLStatement(type, node, sql, resultAction) {           
        node.gaianConfig.jdbc.reserve(function(err,connObj){
          
            var queryHandler = function(err, resultset) {
                if (err) {
                    node.status({fill:"red",shape:"ring",text:"failed"});
                    node.error(err.message);
                    releaseConnection(node, connObj);
                } else {
                    //the query succeeded
                    node.status({fill:"green",shape:"dot",text:"connected"});
                    resultAction(node, resultset, connObj);
                }
            };                       
           
            if (err) {
                // connection failed.
                node.status({fill:"red",shape:"ring",text:"disconnected"});
                node.error("Could not connect to gaian database.");
            } else if (connObj) {
                // we got a connection from the pool, but it could still be "unusable" if the database is now unreachable. 
                var conn = connObj.conn;
                conn.createStatement(function(err, statement) {
                    if (err) {
                        // connection failed when we try to create statement - possibly if the gaiandb is shutdown.
                        node.status({fill:"red",shape:"ring",text:"disconnected"});
                        node.error(err.message);
                        releaseConnection(node, connObj);
                        node.gaianConfig.jdbc.purge(function(err) {
                            if (err) {
                                node.error(err.message);
                            }
                        });
                    } else {
                        // Connection and statement are OK, now try and execute the sql.
                    	if (type=="Query"){
                    		statement.executeQuery(sql, queryHandler);
                    	} else if (type=="Update"){
                    		statement.executeUpdate(sql, queryHandler);
                    	} 
                    }
                });
            }
        });
    }
    
    // This function processes a SQL statement on a config node, providing
    // common connection handling and error processing. The "type" should be "Query" or "Update" 
    // the resultAction is a function provided to handle a successful query result.
    function processConfigSQLStatement(type, configNode, sql, resultAction, errorAction) { 
        // define function to handle a reserved connection 
        function reserveConnectionHandler (err,connObj){
          
            var queryHandler = function(err, resultset) {
                if (err) {
                    errorAction(configNode);
                    releaseConnection(configNode, connObj);
                } else {
                    //the query succeeded
                    resultAction(configNode, resultset, connObj);
                }
            };                       
           
            if (err) {
                // connection failed.
                errorAction(configNode);
            } else if (connObj) {
                // we got a connection from the pool, but it could still be "unusable" if the database is now unreachable. 
                var conn = connObj.conn;
                conn.createStatement(function(err, statement) {
                    if (err) {
                        // connection failed when we try to create statement - possibly if the gaiandb is shutdown.
                        errorAction(configNode);                        
                        releaseConnection(configNode, connObj);
                        configNode.gaianConfig.jdbc.purge(function(err) {
                            if (err) {
                                configNode.error(err.message);
                            }
                        });
                    } else {
                        // Connection and statement are OK, now try and execute the sql.
                    	if (type=="Query"){
                    		statement.executeQuery(sql, queryHandler);
                    	} else if (type=="Update"){
                    		statement.executeUpdate(sql, queryHandler);
                    	} 
                    }
                });
            }
        }
        
        try{
            configNode.jdbc.reserve(reserveConnectionHandler);
        } catch (err) {
            configNode.error(err.message);           
        }
    }
    
    // Test a database connection for a flow node, setting the "connected" or "disconnected" status icon of the node.
    function testConnection(node, jdbc) {           
        jdbc.reserve(function(err,connObj){
          
            if (err) {
                // connection failed.
                node.status({fill:"red",shape:"ring",text:"disconnected"});
                node.error(err.message);
            } else if (connObj) {
                // we got a connection from the pool, but it could still be "unusable" if the database is now unreachable. 
                var conn = connObj.conn;
                conn.createStatement(function(err, statement) {
                    if (err) {
                        // connection failed when we try to create statement - possibly if the gaiandb is shutdown.
                        node.status({fill:"red",shape:"ring",text:"disconnected"});
                        node.error(err.message);
                    } else {
                        // the connection succeeded
                        node.status({fill:"green",shape:"dot",text:"connected"});
                    }
                    releaseConnection(node, connObj);
                });
            }
        });
    }
    
    // Test connection on a configuration  node, setting the "connected" flag true or false.
    function testConfigConnection(configNode) {           
    	configNode.jdbc.reserve(function(err,connObj){
          
            if (err) {
                // connection failed.
            	configNode.connected=false;
            } else if (connObj) {
                // we got a connection from the pool, but it could still be "unusable" if the database is now unreachable. 
//                            console.log("Using connection: " + connObj.uuid);
                var conn = connObj.conn;
                conn.createStatement(function(err, statement) {
                    if (err) {
                        // connection failed when we try to create statement - possibly if the gaiandb is shutdown.
                    	configNode.connected=false;
                        node.error(err.message);

                    } else {
                        //the connection succeeded
                    	configNode.connected=true;

                    }
                    configNode.jdbc.release(connObj,function(err) {});
                });
            }
        });
    }
    
    // Sets the connection status icon on a flow node based on the status of the config.
    // this doesn't work initially though as the asyncronous callbacks mean that the 
    // connection is not made before the flow nodes are created.
    function setInitialConnectionStatus(node){
    	if(node.gaianConfig.connected){
            node.status({fill:"green",shape:"dot",text:"connected"});
    	} else {
            node.status({fill:"red",shape:"ring",text:"disconnected"});
    	}
    }
    
    // define a function to handle successful query results.
    var queryResultAction = function (node, resultset, connObj) {
        resultset.toObjArray(function(err,results) {
            if(node.multi=="individual"){
                for    (var index = 0; index < results.length; index++) {
                    node.send({"payload": results[index]});
                }
            } else {
                 node.send({ "payload": results });
            }
            releaseConnection(node, connObj);
        })
    };
    
    // define a function to handle successful update results.
    var updateResultAction = function (node, count, connObj) {
        node.send({"payload": {"count" : count}});
        releaseConnection(node, connObj);
    };
    
    
    // Initial stub code to lookup and automatically populate a logical table list
    // from the connected database. To Be Completed
    var listlts = function (nodeID){
        processSQLStatement("Query", node, sql, queryResultAction)
        var list = new Array();;
        list.push('LT0');
        list.push('LT1');
        
        return (list);
    };
    
    var listderbytables = function (nodeID){
        var list = new Array();;
        list.push('localsensorreadings');
        
        return (list);
    };
    
    // define logical tables that we don't want displayed in the list to users
    var LTsNotToDisplay = "DERBY_TABLES,GDB_LTLOG,GDB_LTNULL,GDB_LOCAL_METRICS,GDB_LOCAL_QUERIES,GDB_LOCAL_QUERY_FIELDS".split(",");
    
    RED.httpAdmin.get('/gaiandb/lts', function(req, res) {
        var dbconfignode = null;
        if (req.query.hasOwnProperty("confignodeid")) { 
            dbconfignode = RED.nodes.getNode(req.query.confignodeid);
        }

        if (dbconfignode){
            var sql = "select LTNAME from new com.ibm.db2j.GaianQuery('call listlts()','maxdepth=0') gq union select distinct tabname as LTNAME from derby_tables_0 where tabtype='T'"
            processConfigSQLStatement("Query", dbconfignode, sql, function (node, resultset, connObj) {
                resultset.toObjArray(function(err,results) {
                    var tablelist = new Array();

                    for (var index = 0; index < results.length; index++) {
                        var result = results[index];
 						if (result.hasOwnProperty("LTNAME")){
							// check for tables that we don't want to return
                            if (LTsNotToDisplay.indexOf(result.LTNAME) == -1) {        
                                tablelist.push(result.LTNAME);  
                            }
						}
					}
                    res.json(tablelist);
                })
            },
            function (node) {
                res.json([]);
            })
        } else {
            // no results to return
            res.json([]);
        }
    });
    
    // define logical tables that we don't want displayed in the list to users
    var TablesNotToDisplay = "GDB_LOCAL_METRICS,GDB_LOCAL_QUERIES,GDB_LOCAL_QUERY_FIELDS".split(",");
     
    RED.httpAdmin.get('/gaiandb/tables', function(req, res) {
        var dbconfignode = null;
        if (req.query.hasOwnProperty("confignodeid")) { 
            dbconfignode = RED.nodes.getNode(req.query.confignodeid);
        }

        if (dbconfignode){
            var sql = "select distinct tabname from derby_tables_0 where tabtype='T'";
            processConfigSQLStatement("Query", dbconfignode, sql, function (node, resultset, connObj) {
                resultset.toObjArray(function(err,results) {
                    var tablelist = new Array();

                    for (var index = 0; index < results.length; index++) {
                        var result = results[index];
 						if (result.hasOwnProperty("TABNAME")){
							// check for tables that we don't want to return
                            if (TablesNotToDisplay.indexOf(result.TABNAME) == -1) {      
                                tablelist.push(result.TABNAME);  
                            }
						}
					}
                    res.json(tablelist);
                })
            },
            function (node) {
                res.json([]);
            })
        } else {
            // no results to return
            res.json([]);
        }
    });

    // Define the server configuration node, handling connection details to a Gaian database. 
    function GaianNode(config) {
        
        RED.nodes.createNode(this,config);
        this.hostname = config.hostname;
        this.port = config.port;
        this.ssl = config.ssl;
        this.db = config.db;
        this.name = config.name;
        
        var jdbc = require('jdbc');
        var jinst = require('jdbc/lib/jinst');
        
        // check that the jar file exists!
        try {
            var fs = require('fs');
            fs.accessSync(path.join(__dirname,'jars','derbyclient.jar'), fs.F_OK);
            // OK, no problem
        } catch (e) {
            // It isn't accessible, log an error and return
            this.error ("derbyclient.jar file is missing - see node-red-node-gaiandb install documentation.");
            return;
        }


        // add in the derby class jar into the jvm classpath.
        if (!jinst.isJvmCreated()) {
            jinst.addOption("-Xrs");
            jinst.setupClasspath([path.join(__dirname,'jars','derbyclient.jar')]);
        }
        
        this.url = "jdbc:derby://"+this.hostname+":"+this.port+"/"+this.db+";user="+this.credentials.user+";password="+this.credentials.password;
        
        // add on any ssl options to the connection url.
        switch (this.ssl) {
            case "basic":
                this.url += ";ssl=basic";
                break;
            case "peer":
                this.url += ";ssl=peerAuthentication";
                break;               
        }
        
        if (this.credentials && this.credentials.user && this.credentials.password) {
            this.jdbcconfig={
                libpath: path.join(__dirname,'jars','derbyclient.jar'),
                drivername: 'org.apache.derby.jdbc.ClientDriver',
                minpoolsize: 10,
                maxpoolsize: 100,
                url: this.url,
                user: this.credentials.user,
                password: this.credentials.password
            };
            // initiate connection details to this database instance. Actual connections will be 
            // created by the individual flow nodes.
            this.jdbc= new jdbc(this.jdbcconfig);
        
            testConfigConnection(this);
        } else {
            node.warn("Please provide gaiandb logon credentials.")
        }


    }

    RED.nodes.registerType("gaiandb",GaianNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

    // Define the Gaian input node, handling select and count queries to a Gaian database. 
    function GaianInNode(config) {
        RED.nodes.createNode(this,config);
        if (config.table) {
            this.table = config.table;
        } else if (config.logicaltable) {
            this.table = config.logicaltable;
        } 
        this.gaiandb = config.gaiandb;
        this.operation = config.operation || "select";
        this.multi = config.multi || "individual";
        this.gaianConfig = RED.nodes.getNode(this.gaiandb);

        // define a function to handle successful query results.
        var resultAction = function (node, resultset, connObj) {
            resultset.toObjArray(function(err,results) {
                if(node.multi=="individual"){
                    for (var index = 0; index < results.length; index++) {
                        node.send({"payload": results[index]});
                    }
                } else {
                    msg.payload = results;
                    node.send(msg);
                }
                releaseConnection(node, connObj);
            })
        }
        
        if (this.gaianConfig) {
            var node = this;

            // function defining the actions when each node red message is received.
            // we extract relevant details and execute a query statement.
            node.on("input", function(msg) {
            	var local_jdbc;
                
                //establish which logical table to query
                var table
                if (node.table) table = mustache.render(node.table,msg);;
 
                // form the sql statement
                var selector;
                if (msg.filter) {
                    selector="where "+ msg.filter;
                } else {
                    selector="";
                }
                    
                var sql;
                if (!table) {
                    node.warn("No Table specified")
                } else if (node.operation === "select") {
                    var projection = msg.projection || "*";
                    sql = "SELECT " + projection + " FROM "+table+" "+selector;
                } else if (node.operation === "count") {
                    sql = "SELECT count(*) FROM "+table+" "+selector;
                } else {
                    node.warn("Unrecognised Operation")
                }
                
                if (sql){
	                // now process the sql statement, with necessary error handling.
                   processSQLStatement("Query", node, sql, queryResultAction)

                }
            });

        } else {
            this.status({fill:"red",shape:"ring",text:"disconnected"});
            this.error("No connection to gaiandb.");
        }
    }
    RED.nodes.registerType("gaiandb in",GaianInNode);
    

    // Define the Gaian output node, handling insert, update and delete statements to a Gaian database. 
    function GaianOutNode(config) {
        RED.nodes.createNode(this,config);
        if (config.table) {
            this.table = config.table;
        } else if (config.logicaltable) {
            this.table = config.logicaltable;
        } 
        this.gaiandb = config.gaiandb;
        this.payonly = config.payonly || false;
        this.upsert = config.upsert || false;
        this.multi = config.multi || false;
        this.operation = config.operation;
        this.gaianConfig = RED.nodes.getNode(this.gaiandb);

        if (this.gaianConfig) {
            var node = this;

            // function defining the actions when each node red message is received.
            // we extract relevant details and execute an update statement.
            node.on("input",function(msg) {
             
                //establish which table to query                
                var table
                if (node.table) table = mustache.render(node.table,msg);;
            
                var selector;
                if (!msg.hasOwnProperty("filter")) {
                    selector = null;
                } else if (msg.filter=="") {
                    selector="";
                } else {
                    selector="where "+ msg.filter;
                } 
                
                // form the sql statement
                var sql;
                if (!table) {
                        node.warn("No Table specified")
                } else if (node.operation === "insert") {
                    sql = "INSERT INTO "+table+" VALUES "+ msg.payload;
                 } else if (selector!=null) {
                     // these operations need a valid selector
                    if (node.operation === "update") {
                        sql = "UPDATE "+table+" SET "+ msg.payload + " " + selector;
                    } else if (node.operation === "delete") {
                        sql = "DELETE from "+table+" "+ selector;
                    } else {
                        node.warn("Unrecognised Operation")
                    }
                } else {
                    // no selector
                    node.warn("no msg.filter specified");                    
                }
            
                if (sql){
	                // now process the sql statement, with necessary error handling.
	                processSQLStatement("Update", node, sql, updateResultAction);
                }
            });

        } else {
        	// This can happen if the gaian configuration node cannot be created for some reason. 
            this.status({fill:"red",shape:"ring",text:"disconnected"});
            this.error("No connection to gaiandb.");
        }
    }
    RED.nodes.registerType("gaiandb out",GaianOutNode);

    // Define the Gaian SQL node, handling general SQL query statements to a Gaian database. 
   function GaianSQLNode(config) {
        RED.nodes.createNode(this,config);
        this.query = config.query;
        this.multi = config.multi || "individual";
        this.gaiandb = config.gaiandb;
        this.gaianConfig = RED.nodes.getNode(this.gaiandb);

        if (this.gaianConfig) {
            var node = this;
          
            // function defining the actions when each node red message is received.
            // we extract relevant details and execute a database query.
            node.on("input", function(msg) {
 
                //establish the query string
                var query;
                if (node.query) {
                    query = node.query;
                } else if (msg.payload) {
                    query = msg.payload;
                } else {
                    node.warn("No query defined");
                    return;
                }

                // resolve any "mustache" tags in the query, potentially substituting elements of the message (msg) into the query.
                var sql = mustache.render(query,msg);

                // now process the SQL statement, with necessary error handling.
                processSQLStatement("Query", node, sql, queryResultAction)
            });

        } else {
        	// This can happen if the gaian configuration node cannot be created for some reason. 
            this.status({fill:"red",shape:"ring",text:"disconnected"});
            this.error("No connection to gaiandb.");
        }

    }
    RED.nodes.registerType("gaiandb sql",GaianSQLNode);
}
