
# coding: utf-8

# 
# + Hadoop est un framework pour le stockage distribué ( HDFS ) et le traitement distribué ( YARN ).
# 
# + **Apache Hive** is un data warehouse et outil  ETL  au dessus de la Apache Hadoop,  nous y avons crée nos bases de dedons. Donc nous avons besoin d'un context hive
# 
# + Spark est un moteur de calcul distribué en mémoire.
# 
# + Spark peut fonctionner avec ou sans composants Hadoop (HDFS/YARN)
# 
# + Spark n'est pas lié au paradigme MapReduce en deux étapes et promet des performances jusqu'à 100 fois plus rapides que Hadoop MapReduce pour certaines applications
# 
# + Le SparkContext peut se connecter à plusieurs types de cluster manager (local, Mesos ou YARN)


from pyspark import SparkContext, HiveContext, SparkConf 


# # Configuration
# 
# Ceci est  pour exécuter le travail en mode local.
# 
# Il est utilisé pour tester le code dans une petite quantité de données dans un environnement local
# 
# Il ne fournit pas assez d' avantages de l'environnement distribué surtout dans la gestions des ressources
# 
# *  "*" est le nombre de cores à disponible à alloueer


conf = SparkConf().setMaster('local[*]')                # setMaster('yarn-client')
conf = conf.setAppName('my_second_app')
conf =  conf.set('spark.ui.port', '5050')               # definition du uri spark
conf= conf.set('spark.sql.shuffle.partitions','4')
sc = SparkContext(conf=conf)                            # Instanciation du context spark
hctx = HiveContext(sc)                                  # Instanciation du context hive



# sc.stop()                                             # arret du context spark


# # Lecture de la table de donnée depuis Hive


df = hctx.sql("SELECT  idnt_comp_serv, flg_rsil, anc_actv,                       val_debit_down, libl_res, libl_tech, nb_iad, iad_anc_info, iad_userkey2, fdate_iad_anc,                       iad_model, iad_vendor,     iad_softwarever, rk_asc_iad_conf, nb_stv, stb_anc_info,                       fdate_stb_anc, stb_model, stb_vendor, stb_softwarever, rk_asc_stb_conf, noinadinfo, nostbinfo               FROM upec_2022.iad_stb_model A  WHERE rdn_samp=1")



df.show(2)


# Les classses sont très déséquilibres, le taux d'erreur n'est plus pertinent pour mésurer la qualité du modèle.
# 
# En effet il y a 98% et 2%. Une des solutions serait de rééchantillonner les données. 
# 
# C'est pourquoi df revient à selectionner les rdn_samp = 1


hctx.sql("SELECT FLG_RSIL, COUNT(*) FROM upec_2022.iad_stb_model GROUP BY FLG_RSIL").show() 


# La table qui nous interesse est alors un tirage aléatoire de 100 % parmi les postitfs et 10 % parmi les negatifs


hctx.sql("SELECT FLG_RSIL, COUNT(*)              FROM upec_2022.iad_stb_model           WHERE rdn_samp =1                     GROUP BY FLG_RSIL").show() 


# show() est une méthode qui s'applique au df, le hive context converti toute les tables en dataframe


type(hctx.sql("SELECT FLG_RSIL, COUNT(*)              FROM upec_2022.iad_stb_model           WHERE rdn_samp =1                     GROUP BY FLG_RSIL"))



type(df)


# # Controle du nombre de partitions

# RDD (Resilient Distributed Dataset) est un objet natif de spark.
# 
# C' est une structure de données en mémoire utilisée par Spark. très rapide car plus besoin d'aller lire les données sur hdfs 
# 
# Une fois que le context spark  s'arrette ', il n'y a plus de RDD
# 
#  On peut changer le partitonning du dataset pour s'adapter à la puissance qu'on a, je n'ai que quatre cpus logiques


df.rdd.getNumPartitions()


rdd = df.rdd.repartition(4)
type(rdd)


# Les méthodes take() , collect() , first renvoient des objets python (list, row


type(rdd.take(2)), type(rdd.collect())


# Le **lazy execution** dans Spark signifie que l'exécution ne démarrera pas tant qu'une action ne sera pas déclenchée.  Par exemple, df.rdd va d'abord revenir en arrière sur le code qui permet d'avoir df, une fois exécutée, alors on le transform en rdd. Donc cela est très couteux en temps de calcul.
# 
# **Cache()**  et **persist()** sont des techniques d'optimisation pour les applications Spark  afin d'améliorer les performances des jos  des applications.Ils sont sont utilisés pour enregistrer les Spark RDD, Dataframe et Dataset.
# 
# Mais, la différence est que la méthode RDD cache() l'enregistre par défaut dans la mémoire (MEMORY_ONLY) tandis que la méthode persist() est utilisée pour le stocker au niveau de stockage défini par l'utilisateur.
# 
# Lorsque l'objet à persister est un rdd alors cache () et persist() deviennent pareils. Naturellement rdd.unpersist() existe


rdd = rdd.persist()

rdd.first()


# # Gestion de doublons


#CONTROLE ET GESTION DES DOUBLONS
NonUnique = rdd.map(lambda x:  (int(x.idnt_comp_serv),1))                .reduceByKey(lambda x,y : x+y)                .filter(lambda x : x[1]>1)                .keys().collect()


print(NonUnique)


rdd = rdd.filter(lambda x : int (x.idnt_comp_serv)  not in NonUnique)
rdd.persist()


# # Data processing

# Un modèle comme le Random Forest (basés sur les arbres) est robuste aux valeurs manquantes !
# Imputer ici risque de créer des données artificielles et eleminer certaines rélations 
# Une solution est de considerer la valeur manquante comme une modalité.

# ## Cas des variables quantitatives et valeurs manquantes

# Ici assongons -1 lorsqu'il s'agit de NaN


# Fonctions
def MissingRecode(arr) :
    return [-1 if v is None else v for v in arr]


# Récuperations des Champs utiles
train = rdd.map(lambda x: (x.idnt_comp_serv, (x.idnt_comp_serv, x.flg_rsil, x.anc_actv, x.val_debit_down, x.iad_anc_info, 
                                              x.nb_iad, x.fdate_iad_anc, x.rk_asc_iad_conf,x.stb_anc_info, x.nb_stv,
                                              x.fdate_stb_anc,  x.rk_asc_stb_conf, x.noinadinfo ,x.nostbinfo)
                          ))



train = train.map(lambda x : (x[0],MissingRecode(x[1])))
train =train.persist()


# nombre totales de features
NQuantvar = train.map(lambda x : len(x[1])).first()
print NQuantvar-2


# ## Traitement des variables catégorielles

def JoinListAppender(aList,aValue):
    aList.append(aValue)
    return tuple(aList)

'''
A.join(B) => (KEY, (VA,VB))
C.join(D) => (KEY, ((VA,VB),VC))
D.join(E) => (KEY, (((VA,VB),VC),VD))
'''


# On rappelle que df est un dataframe

df.dtypes

df.columns.index('noinadinfo')


# In[25]:


#Encodage des variables catégorielles (de texte)  en entier (integer)
for i in df.dtypes:
    if i[1]=='string':
        # Recuperer l'index de la colonne
        j = int(df.columns.index(i[0]))
        #  Recuperer la list des valeurs uniques de chaque variable categorielle
        valList = rdd.map(lambda x : (x[j],1)).groupByKey().sortByKey().map(lambda x :x[0]).collect()
        # Retourner la clée et la colonne (nouvelle )
        recode = rdd.map(lambda x : (x.idnt_comp_serv,valList.index(x[j])))
        #  jointure à la table rdd de la colum
        train = train.join(recode).map(lambda x: (x[0],JoinListAppender(list((x[1][0])),x[1][1])))


rdd.unpersist()
train=train.repartition(4).persist()
train.getNumPartitions()


# # Analyse prédictive avec RF

# In[27]:


from pyspark.mllib.linalg import DenseVector
from pyspark.mllib.regression import LabeledPoint


# ## Préparation de la table pour MLlib
# 

# ### LabeledPoint

# On parle de vecteur parse lorsque vous avez beaucoup de valeurs dans le vecteur comme zéro Alors qu'on parle de vcteur dense lorsque la plupart des valeurs du vecteur sont non nulles 

# **vecteur dense**
# 
# vd= [0,1,0,3,4,0,5,0,0,0,0,0,0,0,0,0,0,0,0,12]
# 
# 
# **vecteur sparse** --> Optimisation de memoire'''
# 
# vs = [{1:1},{3:3},{6:5},{19:12}]            # couple indice valeurs, les indices non présentes ont par défintions des  valeurs
# nulles


Nvar=train.map(lambda x: len(x[1])).first()
print Nvar


# Pour entrainer  un nombre , nous devrions instancier une Classe labeledpoint qui représente pour chaque individu le labels (indice =0) et  features indice 1 , dont le contenu est  une liste).
# 
# Afin d'identifier les individus la notion de clé valeurs (ici le labeledpoint) reste d'actualité


lp =train.map(lambda x : (x[0],LabeledPoint(float(x[1][1]),DenseVector([x[1][i] for i in range(2,Nvar)])))) 
lp=lp.persist()
lp.take(3)


lp.map(lambda x : (x[1].features)).first()


lp.map(lambda x : (x[1].label)).first()


# ### Déclarations de features catégorielles

# Nous dévrions informer à Spark les features qui sont catégorielles afin qu'il puisse les traiter differemment.
# 
# Pour cela , il faut créer un dictionnaire python qui retournera les features et leurs nombres de modalités


catFeatInfo = {} # on a créer un dictionnaire pour que spark comprenne quelle sont les var text et numérique
for i in range(NQuantvar-1,Nvar-2):
    catBins =lp.map(lambda x: x[1].features[i]).distinct().count()
    catFeatInfo.update(dict({i:catBins}))
print catFeatInfo


# ### Creation d'un échantillon train et test


(lpTrain,lpTest)=lp.randomSplit([0.8,0.2])
print("N apprentissage %d N -Test %d" %(lpTrain.count(),lpTest.count()))


# ## Training

# In[34]:


from pyspark.mllib.tree import RandomForest

lpTrain.persist()
RNFModel = RandomForest.trainRegressor(lpTrain.map(lambda x: x[1]),
                                      categoricalFeaturesInfo=catFeatInfo,
                                      numTrees=10, featureSubsetStrategy="sqrt",
                                      impurity='variance',maxDepth=4,maxBins=95,
                                      seed=2022)


# ## Sauvegarde du modèle sur hdfs et importations


#exportation
RNFModel.save(sc,"/user/hadoop/MODELS/RNF_models_2022")


#Importation
from pyspark.mllib.tree import *
Model = RandomForestModel.load(sc, "/user/hadoop/MODELS/RNF_models_2022")
print Model


df.groupBy("flg_rsil").count().show()


# In[41]:


#Apply model to test
Preds = RNFModel.predict(lpTest.map(lambda x: x[1].features))
Preds.stats()
#Taux d'évènement à 12% donc c'est bien ça rpz notre % de churner dans notre table


Preds.stats()



Preds.take(2)


# ## Courbe Lift

# Le lift a un rôle de ciblage : solliciter les clients les plus réceptifs
# 
# • optimiser un budget limité
# 
# • ne pas agacer les clients « hostiles »
# 
# Les étapes :
# 
# 
# 1. Appliquer la fonction score sur le reste de la base
# 2. Trier la base selon le score
# 3. Cibler en priorité les clients à fort score(les plus appétents en premier. Les moins appétents en derniers)
# 4. Prévoir les performances à partir de la courbe LIFT

# Rattachons les individus à leur prédictions
# 
# - x[0][0] = id_client
# - x[0][1] =  label
# - x[1] = prédictions


kPreds=lpTest.map(lambda x : (x[0], x[1].label)).zip(Preds).zip(Preds).map(lambda x : (x[0][0],x[0][1], x[1]))
kPreds.take(3)


# In[45]:


lpTest.map(lambda x : (x[0], x[1].label)).zip(Preds).first()


Nrows = kPreds.count()
Tres = kPreds.map(lambda x : x[1]).reduce(lambda x,y :x+y)
print(Nrows, Tres)



# ordonnées les probabilités : ((0.54436346, 0.54436346054), 0)
# Fréquence d'individus, (1, probabilité)
# Par fréquence de 1%: compter le nombre d'individus, somme des probabilité
# Par fréquence de 1%: compter le nombre d'individus, somme des probabilité , le ratio somme des proba/ somme totale des proba



res = kPreds.map(lambda x: (x[2], x[1])).sortByKey(ascending=False).zipWithIndex()               .map(lambda x : (x[1]*100/Nrows, (1, x[0][1]))).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))               .map(lambda x : (x[0], x[1][0], x[1][1], x[1][1]*1.00/Tres))


res.takeOrdered(100, lambda x : x[0])




cutOff = 10
Lift = res.filter(lambda x :x[0]<cutOff).map(lambda x: x[3]).sum()/(cutOff/100.00)
print Lift


# # OPTIMISATION DES PARAMETRE DU MODELE



# Optimisation des paramètres du modele
import time




#lpTrain.persist()
cutOff = 10

for i in (10,20,50):
    for j in (6, 8, 10, 16):
        start_time = time.time()
        RNFModel = RandomForest.trainRegressor(lpTrain.map(lambda x: x[1]),
                                               categoricalFeaturesInfo= catFeatInfo,
                                               numTrees=i,
                                               featureSubsetStrategy="sqrt",
                                               impurity='variance',
                                               maxDepth=j,
                                               maxBins=95,
                                               seed=12345)
        Preds = RNFModel.predict(lpTest.map(lambda x: x[1].features))
        kPreds = lpTest.map(lambda x: (x[0], x[1].label)).zip(Preds).map(lambda x : (x[0][0], x[0][1], x[1]))
        res = kPreds.map(lambda x: (x[2], x[1])).sortByKey(ascending=False).zipWithIndex()               .map(lambda x : (x[1]*100/Nrows,(1, x[0][1]))).reduceByKey(lambda x,y : (x[0]+ y[0], x[1]+y[1]))               .map(lambda x : (x[0], x[1][0], x[1][1], x[1][1]*1.00/Tres))
        Lift = res.filter(lambda x :x[0] < cutOff).map(lambda x: x[3]).sum()/(cutOff/100.00)
        elapsed = time.time() - start_time
        print ("numTrees: %d - Depth : %d - Lift : %f - (%f s.)" %(i,j,Lift,elapsed))

