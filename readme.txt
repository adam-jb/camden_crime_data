

# to recreate the documents here, run the following in the command line 
# note process_data.py takes 5-10 mins to run, as it's requesting and processing a large amount of data
pip3 install -r requirements.txt
python3 process_data.py
python3 make_documents.py
