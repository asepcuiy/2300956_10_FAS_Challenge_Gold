import pandas as pd
import sqlite3
import string
import os
import re
from flask import Flask, jsonify
from flask import request
from flasgger import Swagger, LazyString, LazyJSONEncoder
from flasgger import swag_from



app = Flask(__name__)


app.json_provider_class = LazyJSONEncoder
app.json = LazyJSONEncoder(app)

swagger_template = dict(
    info={
        'title': LazyString(lambda: 'API Documentation for Data Processing and Modeling'),
        'version': LazyString(lambda: '1.0.0'),
        'description': LazyString(lambda: 'Dokumentasi API untuk Data Processing dan Modeling'),
    },
    host=LazyString(lambda: request.host)
)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'docs',
            "route": '/docs.json',
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/"
}

swagger = Swagger(app, template=swagger_template, config=swagger_config)


# pandas dataframe for kamusalay
df_kamusalay = pd.read_csv('data/new_kamusalay.csv',
                           encoding='latin-1', names=['find', 'replace'])
# Mapping for kamusalay
kamusalay_mapping = dict(zip(df_kamusalay['find'], df_kamusalay['replace']))


# pandas dataframe for kamusabusive
df_kamusabusive = pd.read_csv('data/abusive.csv',
                              encoding='latin-1',
                              names=['find', 'replace'],
                              skiprows=1)

# Mapping for kamusabusive
kamusabusive_mapping = dict(zip(df_kamusabusive['find'], ['' for _ in df_kamusabusive['find']]))


# merubah kalimat menjadi huruf kecil
def text_lower(text):
    text = text.lower()
    return text


# text cleansing by regex
def remove_unnecessary_char(text):
    text = re.sub(r'[^a-z ]', ' ', text)  
    text = re.sub(r'  +', ' ', text)  
    text = re.sub(r'\\+n', ' ', text)  
    text = re.sub(r'\n', " ", text)  
    text = re.sub(r'(rt)', ' ', text)  
    text = re.sub(r'\\x.{2}', ' ', text)
    text = re.sub('user', ' ', text)
    text = re.sub(r'&amp;', 'dan', text)  
    text = re.sub(r'&', 'dan', text)   
    text = re.sub(r'((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))', ' ', text)    
    text = re.sub(r'\+62\d{2,}', ' ', text)   
    text = re.sub('[\+\d{5}\-\d{4}\-\d{4}]', ' ', text)    
    text = re.sub(r'%', ' persen ', text)  
    text = re.sub('[%s]' % re.escape(string.punctuation),
            ' ', text)  
    text = text.rstrip().lstrip()  

    return text


# Mereplace kata Alay
def handle_from_kamusalay(text):
    wordlist = text.split()
    clean_alay = ' '.join([kamusalay_mapping.get(x, x) for x in wordlist])
    return clean_alay

# Mereplace kata abusive
def handle_from_kamusabusive(text):
    wordlist = text.split()
    clean_abusive = ' '.join([kamusabusive_mapping.get(x, x) for x in wordlist])
    return clean_abusive


# Membersihkan data dan kamus alay
def apply_cleansing_file(data):
    # menghapus duplikasi data
    data = data.drop_duplicates()
    # merubah menjadi huruf kecil
    data['text_lower'] = data['Tweet'].apply(lambda x: text_lower(x))
    # drop kolom tweet
    data.drop(['Tweet'], axis=1, inplace=True)
    # implement menghapus_unnecessary_char function
    data['text_clean'] = data['text_lower'].apply(
        lambda x: remove_unnecessary_char(x))
    # apply kamusalay function
    data['Tweet'] = data['text_clean'].apply(lambda x: handle_from_kamusalay(x))
    # apply kamusabusive function
    data['Tweet'] = data['text_clean'].apply(lambda x: handle_from_kamusabusive(x))
    # drop text clean column
    data.drop(['text_lower', 'text_clean'], axis=1, inplace=True)

    return data

# Membuat database dengan sqlite
def create_database_text(text):
    if not os.path.exists("result"):
        os.makedirs("result")
    conn = sqlite3.connect("result/data_text_result.db")
    conn.execute("CREATE TABLE if not exists tweet(text VARCHAR)")
    conn.execute("INSERT INTO tweet VALUES (?)", (text,))
    conn.commit()
    conn.close()


# Membuat hasil database
def create_database_file(data):
    if not os.path.exists("result"):
        os.makedirs("result")

    conn = sqlite3.connect('result/data_text_result.db')

    df = pd.DataFrame(data={"text": data})

    df.to_sql('tweet',  
                con=conn,  
                if_exists='append',  
                index=False
                )


# ROUTE
# text processing
@swag_from("docs/text_processing.yml", methods=['POST'])
@app.route('/text-processing', methods=['POST'])
def text_processing():

    text = request.form.get('text')

    json_response = {
        'status_code': 200,
        'description': "Teks yang sudah diproses",
        'data': re.sub(r'[^a-zA-Z0-9]', ' ', text),
    }

    create_database_text(text)

    response_data = jsonify(json_response)
    return response_data


# File processing
@swag_from("docs/text_processing_file.yml", methods=['POST'])
@app.route('/text-processing-file', methods=['POST'])

# processing text route
def text_processing_file():
    file = request.files.getlist('file')[0]
    df = pd.read_csv(file, sep=",", encoding="latin-1")

    assert any(df.columns == 'Tweet')
   
    df = apply_cleansing_file(df)

    texts = df.Tweet.to_list()

    cleaned_text = []
    for text in texts:
        cleaned_text.append(text)

    json_response = {
        'status_code': 200,
        'description': "Teks yang sudah diproses",
        'data': cleaned_text
    }

    create_database_file(cleaned_text)

    response_data = jsonify(json_response)
    return response_data


if __name__ == '__main__':
    app.run()
