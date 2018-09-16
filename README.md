# smart-onion
Machine learning and artificial intelligence layer for Security-Onion

# General idea:
The general idea in this project is to build a set of services that will automatically analyse the outputs 
generated by SecurityOnion by using machine learning, anomaly detection and event correlation to detect Cyber
related events. 


# Architecture
The project's architecture is based on the microservices paradigm: <br />
<pre>
<b>+ backend</b> - Contains all the backend services and components
<b>|    |</b>
<b>|    +---+ services</b> - Contains all the background services
<b>|</b>             <b>|</b>
<b>|             +--- alerter</b> - The alerter service is responsible for filtering alerts and aggregating alerts and anomalies into Cyber events
<b>|</b>             <b>|</b>
<b>|             +--- configurator</b> - The configurator service is responsible of allowing the various services to get and set their config as well as configuring the scheduler of the system (Zabbix)
<b>|</b>             <b>|</b>
<b>|             +--- metrics_analyzer</b> - The metrics analyser service should use ML, deep learning, NLP as well as biologically constrained machine learning (nupic) and statistical algorithms to detect anomalies or indications of unexpected behaviour (either human or network or other) and return the probability for an anomaly in a given metric 
<b>|</b>             <b>|</b>
<b>|             +--- metrics_collector</b> - The metrics collector (a.k.a sampler) service is responsible for querying the raw data (from Security Onion's elasticsearch cluster and from helper DB instances if needed) and create metrics and also allow usage of threshold based alerts
<b>|</b>  
<b>+ frontend</b> - Contains all the frontend services responsible for the UX <br />
</pre>

+ Each such service will have its own HTTP listener using the Bottle framework.
+ Each such service will not depend on any of the other services except for the configurator.
+ Each backend service will be developed in Python 3.5.
+ Each frontend service will be developed in NodeJS.
+ Each such service will be responsible ONLY for its task as listed above. 
 

# TODO:
<b>[ ] <u>Backend:</u></b><br />
&nbsp;&nbsp;&nbsp;&nbsp;<b>[ ] metrics_collector:</b><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to test texts similarities by using perceptive hashing.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to test texts similarities by using edit-distance algorithms.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to count records in Elasticsearch based on query.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to retrieve the top/bottom one value from records in Elasticsearch based on query.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to retrieve list of unique values (either in cleartext or base64 encoded) based on query.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to retrieve a hash of a list of unique values based on query.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[x] Add the ability to measure the similarity between a string and the unique values returned from an Elasticsearch query.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add the relevant queries in the system's config file to allow for measuring all the metrics defined in /docs/Metrics.xlsx.<br />
&nbsp;&nbsp;&nbsp;&nbsp;<b>[ ] metrics_analyzer:</b><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add the ability to use clustering algorithms to detect anomalies in metrics collected.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add the ability to use Nupic framework to detect anomalies in metrics collected.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add the ability to use statistical algorithms (Moving-Min, Moving-Max, Moving-Average, Moving-Stdev, etc.) to detect anomalies in metrics collected.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] configurator:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add the ability to use relational database (postgreSQL, mysql, etc.) for storing and retrieving configuration values.<br />
&nbsp;&nbsp;&nbsp;&nbsp;<b>[ ] General infrastructure:</b><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Add a scheduling mechanism (Zabbix?) for quering the various services of this project and creating metrics.<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Test the maturity of the Ceres database for storing metrics and either prefer Ceres or Whisper files.<br />
&nbsp;&nbsp;&nbsp;&nbsp;<b>[ ] alerter:</b><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[ ] Create the alerter microservice. This service will be responsible for conducting an automated investigation of triggered alerts and according to the amount of available information forward alerts with all the relevant data to the analysts.<br />

<b>[ ] <u>Frontend:<br /></u></b>