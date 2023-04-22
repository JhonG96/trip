from traceback import print_exc
from flask import Flask, render_template,request
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import re
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import LabelEncoder, StandardScaler
from IPython.display import display

nltk.download('stopwords')
stopwords = nltk.corpus.stopwords.words('spanish')
data = pd.read_excel('df_alojamientos.xlsx',index_col="Unnamed: 0")
data["total_alojamiento"] = data["Precio"]+data["Transport_selection2"]
keywords=["Casa grande", "piscina","parqueadero","privado","aire acondicionado","BBQ","paisaje","terraza","balcón","jacuzzi","WIFI","bbq","lavadero","asador","Piscina","tranquilo","WiFi"]
data['Keyword'] = data['Descripcion'].str.findall('|'.join(keywords)).apply(set).str.join(', ')
#import pickle 

app = Flask(__name__)
##model = pickle.load(open('model.pkl','rb'))

@app.route("/")
def home():
    return render_template('index.html')

@app.route("/destinos")
def destinos():
    return render_template('destinos.html')

@app.route("/blog")
def blog():
    return render_template('blog.html')

@app.route("/gracias")
def gracias():
    return render_template('gracias.html')


@app.route("/predecir",methods = ['POST'])
def predecir():
    recomendacion = None
    try:
        precio = int(request.form['presupuesto'])
        adultos = int(request.form['adultos'])
        ninos = int(request.form['ninos'])
        Huespedes = adultos + ninos
        recomendacion= recommend(precio, Huespedes)
        for result in list(recomendacion):
            print(result)
    except Exception as e:
        print ('type is:', e.__class__.__name__)
        print_exc()
    return render_template('index.html',alojamientos=recomendacion.tolist())

def number_columns(data):
    df_imoveis = data.copy()
    numeric_columns = ["#Huespedes","# Cuartos","# Camas","# Baños","Calificación",
                       "Precio", "Transport_selection2"]
    for column in numeric_columns:
        df_imoveis[column] = df_imoveis[column].replace(
            '', np.nan).astype(float)

    return df_imoveis

def clean_data(data):
    """Return the a copy of the original dataframe with the
    columns that are in the model and treated for missing values."""
    df_imoveis = number_columns(data)

    columns = [ "Destino","Ubicación","Place", "Descripcion","comentarios","# Cuartos","# Camas",
                       "#Huespedes", "Calificación","Precio", "total_alojamiento","Transport_selection2","Keyword"]
    df_imoveis = df_imoveis[columns]
    df_imoveis['# Camas'] = df_imoveis.groupby(
        'Destino', group_keys=False)['# Camas'].apply(lambda x: x.fillna(x.median()))
    df_imoveis['# Camas'].fillna(0, inplace=True)
    df_imoveis['# Cuartos'] = df_imoveis.groupby(
        'Destino', group_keys=False)['# Cuartos'].apply(lambda x: x.fillna(x.median()))
    df_imoveis['# Cuartos'].fillna(0, inplace=True)
    df_imoveis['Precio'] = df_imoveis.groupby(
        'Destino', group_keys=False)['Precio'].apply(lambda x: x.fillna(x.mean()))
    df_imoveis['Precio'].fillna(0, inplace=True)
    df_imoveis['#Huespedes'].fillna('', inplace=True)
    df_imoveis['Transport_selection2'].fillna('', inplace=True)

    return df_imoveis

def transform_data(data):
    """Return the dataframe with categorical columns
    encoded with label encoder and numerical columns standardized."""
    df_imoveis = clean_data(data)

    le = LabelEncoder()
    categorical_columns = ['Ubicación','Destino','Place']
    for column in categorical_columns:
        df_imoveis[column] = le.fit_transform(df_imoveis[column])
    sc = StandardScaler()
    numerical_columns = ['Calificación', "total_alojamiento"]
    df_imoveis[numerical_columns] = sc.fit_transform(
        df_imoveis[numerical_columns])

    return df_imoveis

def stack_data(data):
    """Return the matrix that contain the similarity score between items.
    It have been used the tfidf vectorizer in order to work with text data.
    The metric that have been chosen it is cosine similarity."""
    df_imoveis = transform_data(data)
    title_metadescription = df_imoveis['Descripcion']
    title_comodidades = df_imoveis['comentarios']
    df_imoveis.drop(['Descripcion', 'comentarios','Keyword'],
                    axis=1, inplace=True)
    title_vec = TfidfVectorizer(
        min_df=10, ngram_range=(1, 3), stop_words=stopwords)
    title_vec2 = TfidfVectorizer(
        min_df=5, ngram_range=(1, 1), stop_words=stopwords)
    title_vec3 = TfidfVectorizer(
        min_df=1, ngram_range=(1, 1), stop_words=stopwords)
    

    title_bow_metadescription = title_vec.fit_transform(title_metadescription)
    title_bow_comodidades = title_vec2.fit_transform(title_comodidades)
    # title_bow_keywords = title_vec3.fit_transform(keywords)
    # print(title_bow_keywords)

    Xtrain_wtitle = hstack(
        [df_imoveis, title_bow_metadescription, title_bow_comodidades])

    nearest_neighbor = cosine_similarity(Xtrain_wtitle, Xtrain_wtitle)


    return nearest_neighbor

def recommend(precio, Huespedes):
  
    nearest_neighbor = stack_data(data)
    columns = ["Destino","Ubicación",'Descripcion','comentarios',"Place", "# Cuartos","# Camas",
                    "#Huespedes", "Calificación","total_alojamiento", "Transport_selection2","Keyword"]
    similar_listing_ids = []
    df_original = data
    df_original.reset_index(drop=True, inplace=True)
    df_original = df_original[df_original['#Huespedes'] == Huespedes]
    try:
      idx = df_original.loc[df_original['total_alojamiento'] <= precio].index[0]

      #creating a Series with the similarity scores in descending order
      score_series = pd.Series(nearest_neighbor[idx]).sort_values(ascending=False)
      df_original['Score'] = score_series
      df_original = df_original.copy()
      df_original = df_original.sort_values(by=['Score'],ascending=False)
      dfNew =df_original.head(5)
      dfNew = dfNew[['Destino','Place','total_alojamiento']].values
      
      

    except Exception as e:
        print ('type is:', e.__class__.__name__)
        print_exc()
      #print('No tenemos resultado ahora')
    
    

    return dfNew

if __name__ == "___main___":
    app.run()