wikia_solr_backend
-------------------------------

This is an updated backend to Wikia's Solr Indexing platform.
It's just one of numerous parts we need for our indexing backend to work.

System Requirements
-------------------------------
* Scribe must be delivering event files to whatever machine this is running on
  (this update adds some separation of concerns to make it easier to move to another queueing system, though!)
* Wikia\Search\IndexService\All must be accessible via wikia.php.
* Solr must be available within the network of the machine where this was deployed

Running The Indexing Script
----------------------------
First, make sure to install the library:

    [me@host solr-backend]$ sudo pip install -r requirements.txt && sudo python setup.py install

Now we can run the event file handler as follows, from anywhere:

    # you could add some options here, use the --help flag to view what they are
    [me@host solr-backend]$ sudo python -m wikia_solr_backend.event_file_handler

This script uses asynchronous multiprocessing to juggle dozens of requests to our application servers, and then
sends update and delete data to Solr. The script is designed to be fault-tolerant with respect to the
application and search endpoints.

Future Work
------------------
The next steps for this script involves plugging English document events to the NLP pipeline.
Plans here are to either use the logging infrastructure introduced, or to add in some asynchronous hooks.