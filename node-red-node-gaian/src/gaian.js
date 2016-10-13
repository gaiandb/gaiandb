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
                node.error(err.message);
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
                node.error(err.message);
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
    }
    
    // define a function to handle successful update results.
    var updateResultAction = function (node, count, connObj) {
        node.send({"payload": {"count" : count}});
        releaseConnection(node, connObj);
    };
    
    
    // Initial stub code to lookup and automatically populate a logical table list
    // from the connected database. To Be Completed
    var listlts = function (nodeID){
        return ("LT0", "LT1");
    };
    
    RED.httpAdmin.get('/gaiandb/lts', function(req, res) {
        res.json(listlts());
    });

    // Define the server configuration node, handling connection details to a Gaian database. 
    function GaianNode(config) {
        
        RED.nodes.createNode(this,config);
        this.hostname = config.hostname;
        this.port = config.port;
        this.db = config.db;
        this.name = config.name;
        
        var jdbc = require('jdbc');
        var jinst = require('jdbc/lib/jinst');

        // add in the derby class jar into the jvm classpath.
        if (!jinst.isJvmCreated()) {
            jinst.addOption("-Xrs");
            jinst.setupClasspath([path.join(__dirname,'jars','derbyclient.jar')]);
        }
        
        this.url = "jdbc:derby://"+this.hostname+":"+this.port+"/"+this.db+";user="+this.credentials.user+";password="+this.credentials.password;
        
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
        } else {
            this.jdbcconfig={
                libpath: './lib/derbyclient.jar',
                drivername: 'org.apache.derby.jdbc.ClientDriver',
                url: this.url,}
        }

        // initiate connection details to this database instance. Actual connections will be 
        // created by the individual flow nodes.
        this.jdbc= new jdbc(this.jdbcconfig);
        
        testConfigConnection(this);
    }

    RED.nodes.registerType("gaian",GaianNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

    // Define the Gaian input node, handling select and count queries to a Gaian database. 
    function GaianInNode(config) {
        RED.nodes.createNode(this,config);
        this.logicaltable = config.logicaltable;
        this.gaian = config.gaian;
        this.operation = config.operation || "select";
        this.multi = config.multi || "individual";
        this.gaianConfig = RED.nodes.getNode(this.gaian);

        // define a function to handle successful query results.
        var resultAction = function (node, resultset, connObj) {
            resultset.toObjArray(function(err,results) {
                if(node.multi=="individual"){
                    for    (var index = 0; index < results.length; index++) {
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
                var logicaltable = mustache.render(node.logicaltable,msg);;

                // form the sql statement
                var selector;
                if (msg.filter) {
                    selector="where "+ msg.filter;
                } else {
                    selector="";
                }
                    
                var sql;

                if (node.operation === "select") {
                    var projection = msg.projection || "*";
                    sql = "SELECT " + projection + " FROM "+logicaltable+" "+selector;
                } else if (node.operation === "count") {
                    sql = "SELECT count(*) FROM "+logicaltable+" "+selector;
                } else {
                    node.warn("Unrecognised Operation")
                }
                
                if (sql){
	                // now process the sql statement, with necessary error handling.
                   processSQLStatement("Query", node, sql, queryResultAction)

                }
            });

        } else {
            this.warn("missing gaian configuration");
        }
    }
    RED.nodes.registerType("gaian in",GaianInNode);
    

    // Define the Gaian output node, handling insert, update and delete statements to a Gaian database. 
    function GaianOutNode(config) {
        RED.nodes.createNode(this,config);
        this.logicaltable = config.logicaltable;
        this.gaian = config.gaian;
        this.payonly = config.payonly || false;
        this.upsert = config.upsert || false;
        this.multi = config.multi || false;
        this.operation = config.operation;
        this.gaianConfig = RED.nodes.getNode(this.gaian);

        if (this.gaianConfig) {
            var node = this;

            // function defining the actions when each node red message is received.
            // we extract relevant details and execute an update statement.
            node.on("input",function(msg) {
             
                //establish which logical table to query
                var logicaltable = mustache.render(node.logicaltable,msg);;
            
                var selector;
                if (msg.filter) {
                    selector="where "+ msg.filter;
                } else {
                    selector="";
                }

                // form the sql statement
                var sql;
                if (node.operation === "insert") {
                    sql = "INSERT INTO "+logicaltable+" VALUES "+ msg.payload;
                } else if (node.operation === "update") {
                    sql = "UPDATE "+logicaltable+" SET "+ msg.payload + " " + selector;
                } else if (node.operation === "delete") {
                    sql = "DELETE from "+logicaltable+" "+ selector;
                } else {
                    node.warn("Unrecognised Operation")
                }
            
                if (sql){
	                // now process the sql statement, with necessary error handling.
	                processSQLStatement("Update", node, sql, updateResultAction);
                }
            });

        } else {
        	// This can happen if the gaian configuration node cannot be created for some reason. 
        	this.warn("missing gaian configuration");
        }
    }
    RED.nodes.registerType("gaian out",GaianOutNode);

    // Define the Gaian SQL node, handling general SQL query statements to a Gaian database. 
   function GaianSQLNode(config) {
        RED.nodes.createNode(this,config);
        this.query = config.query;
        this.multi = config.multi || "individual";
        this.gaian = config.gaian;
        this.gaianConfig = RED.nodes.getNode(this.gaian);

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
            this.warn("missing gaian configuration");
        }

    }
    RED.nodes.registerType("gaian sql",GaianSQLNode);
}
