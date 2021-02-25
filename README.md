# camden_crime_data

Taking data from the Camden gov API, and viewing trends and events. Covers types of crime and collisions. 

# to recreate the documents here, run the following in Terminal
(note process_data.py takes 5-10 mins to run, as it's requesting and processing a large amount of data)
pip3 install -r requirements.txt
python3 process_data.py
python3 make_documents.py
