# Projet-Structure de données---IMdb to MongoDB clusters
### Authors : **Nicolas CARVAL** - **Quentin NAVARRE** - **Bruno PINCET**

Here is our work for the final project of Structure de données pour le Cloud.

In this repository you will find :

- The **Mongo application** in order to run our Vue.
- A **Jupiter Notebook** (Denormalisation.ipng), containing the script for dernomalisation.
- A **Jupiter Notebook** (Performance.ipng), containing the script for measuring performances of different sharding and number of replicas.


### Context:
Given a SQL Database we had to work on it in order to transform it into a NoSQL DB to then create a cluster of virtual machines and test which configurations (1 server or N server, sharding on X or Y) gave the best performance in terms of time to answer.

We had to then create an app in order to visualize queries.

### Visualisation of our APP :

<img src="https://user-images.githubusercontent.com/84092005/147891665-402ae35b-1944-4578-8bf3-a9ac689709f2.png" >


In conclusion our model is reliable when predicting values under 1500 bikes, but we would not recommand to trust it for huge values.


### Acknowledgement : 
in order to do this work we used some contents of shared ressources online that helped us figure out how to present our work.

we took some inspiration from these :
- for CSS :  https://freefrontend.com/css-forms/
- for API : https://towardsdatascience.com/how-to-easily-deploy-machine-learning-models-using-flask-b95af8fe34d4
- for Machine Learning : https://www.kaggle.com/faressayah/linear-regression-house-price-prediction

Thanks for reading !
