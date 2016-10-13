node-red-node-gaian
========================
<a href="http://nodered.org" target="_new">Node-RED</a> nodes to read and write to a <a href="https://github.com/gaiandb" target="_new">Gaian database</a>.

Pre-requisites
--------------

These modules are installed as part of a <a href="http://nodered.org" target="_new">Node-RED</a> environment. Node-RED is a tool for wiring together hardware devices, APIs and online services in new and interesting ways, these Gaian nodes allow Node-Red to use Gaian as a datasource. 

Installation of the Node-Red environment is described here : <a href="http://nodered.org/docs/getting-started/" target="_new">Node-Red Getting Started</a>.

You will need a Gaian Database to connect to, this is available here: <a href="https://github.com/gaiandb" target="_new">Gaian database</a>.

Connection to the GaianDB will require jdbc and java modules. These will be installed by npm in the following "Install" step.

Install
-------

To install from the npm website, run the following command in the Node-RED install home directory of your user, typically ~/.nodered

    npm install node-red-node-gaian
    
To install from a local zip file, run the following command in theNode-RED install home directory of your user:

    npm install {path to file}/node-red-node-gaian-0.0.4.tgz

The derbyclient.jar file containing the JDBC driver is part of the gaian database installation (usually found in {gaian-install-directory}/libs). This should be copied into <b>{node-red-user-directory}/node_modules/node-red-node-gaian/jars</b>.

Usage
-----

Allows basic access to a Gaian database using the JDBC protocol

This node uses SQL to query or update the configured database. By it's very nature it allows SQL injection... so <i>be careful out there...</i>

The result rows can be sent together as an array or as individual messages.

A Server configuration specifies the server, database and credentials to access.

Node Red Nodes
--------------

There are three node red nodes:

  * Gaian in - queries a gaiandb table, returning the data, or a count of the matching rows.
  * Gaian out - can insert, update and remove objects from a chosen local gaian table.
  * Gaian sql - executes any sql query within the gaian environment, allows complex queries.
  
Gaian in
--------

You can choose "Select" or "Count" as the operator, 

Select queries a logical table, allows you to set the SQL where clause and/or the fields to include in the output.

Count returns a count of the number of rows in a logical table or matching a filter condition.

The result is returned in msg.payload. The Output parameter determines whether the results are returned as individual messages or a single array in one message.

In line with standard Gaian Query functionality, specifying table aliases allow a query to be local or global, or to return provenance columns.

* Table: LT0 - returns data from all databases in the Gaian network with the LT0 table.
* Table: LT0_0 - returns data from the single database to which Node Red is connected (this is a local query)
* Table: LT0_P - returns data from all databases in with the LT0 table and includes "provenance columns" identifying from which data source the data originated.

Gaian out
---------

You can choose "Insert", "Update" or "Delete as the operator, 

Insert will insert a new object based on the incoming msg.payload.

Update will modify an existing object or multiple objects to the values in the incoming msg.payload, based on a filter condition which can be specified as the msg.filter property.

Delete will remove objects that match the conditions passed in the incoming message. 

You can either set the table (the modification target of the query) in the node configuration or on the incoming message. A table specified in the configuration will be used in preference to a table in the message .

Gaian sql
---------

Queries a Gaian database based on a configured SQL query string.

The Query can be specified as a template including "mustache" format tags, which are then substituted for values from the input message.

e.g. SELECT * FROM {{{payload.logicaltable}}} WHERE ID = '{{{payload.id}}}'
substitutes msg.payload.logicaltable and msg.payload.id into the query.

e.g. {{{msg.sql}}}
will execute the SQL specified in the sql property of the incoming message, allowing the node red flow to formulate the query.

The result is returned in the outgoing message payload. The results can be returned as individual messages or a single array in one message.

"GaianQuery" functionality, where a query is propagated and executed separately on each node in the Gaian network can be specified using this module. Example queries are:

* select * from new com.ibm.db2j.GaianQuery	('select count(*) c from LT0_0', with_provenance') Q 
* select * from new com.ibm.db2j.GaianQuery('select * from example, lt0 where example.a = lt0.misc', 'with_provenance') GQ

Gaian specific "Stored Procedures" can also be called using this module. Example Queries are: 

* call listlts()	Show logical table definitions in the whole network
* call listconfig()	Show all configuration properties and their values.
* call listspfs()	Show all stored procedures and functions.

Server configurations
---------------------

Each Gaian node has a link to a shared Server configuration. A number of Server configurations can be set to allow access to multiple Gaian databases. Each Server configuration specifies the server, database and credentials to access.






