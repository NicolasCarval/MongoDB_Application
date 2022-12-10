# Projet-Python---Seoul-Bike-Location
### Authors : **Nicolas CARVAL** and **Alexandre BORDAS**

Here is our work for the dataset named "Seoul Bike Sharing Demand".

In this repository you will find :

- A **PDF** report (SeoulBikeRent.pdf), explaining all the in and outs of the project
- A **Jupiter Notebook** (Project.ipng), containing all our work on the dataset (Analysis and Machine Learning)
- A **HTML** file (HTML_Notebook.html), corresponding to the Notebook but in html in order to ease the viewing of every graphs. **click on download, then the raw file is displayed, save the raw to an .html and then you can open it like a web page, it will display everything in correct format**
- A **Python** file (app.py), corresponding to our API, allowing us to make predictions, it is related to the template and static directories, containing .html and .css for interface. (download our repository, decompress model.rar, start the app.py in spyder to run the server, then go to localhost:8989/ in order to use the API)
- A **.RAR** file (model.rar), which stores our Machine learning model created, we had to compress it as it was too big for github.
- A **CSV** file (Seoul_Bike_Data.csv), the dataset.

### Context:
This dataset contains count of public bikes rented at each hour in Seoul Bike sharing System with the corresponding Weather data and Holidays information.

Currently Rental bikes are introduced in many urban cities for the enhancement of mobility comfort. It is important to make the rental bike available and accessible to the public at the right time as it lessens the waiting time. Eventually, providing the city with a stable supply of rental bikes becomes a major concern. The crucial part is the prediction of bike count required at each hour for the stable supply of rental bikes.

Thus we will explore this dataset in order to show the link between the number of bikes rented and the other variables present in the dataset. We will then elaborate a machine learning model in order to provide a way to predict more or less the number ok bikes that could be rented under specific conditions. And finally we'll turn it to an API.

### Visualisation of our API :

<img src="https://user-images.githubusercontent.com/84092005/147891665-402ae35b-1944-4578-8bf3-a9ac689709f2.png" >

###  Visualisation of graphs :

<table>
  <tr>
    <td>
      <img src="https://user-images.githubusercontent.com/84092005/147833281-6052e5ff-8161-4a68-9731-b35c5ee60e70.png" width="500" height="300">
    </td>
    <td>
      <img src="https://user-images.githubusercontent.com/84092005/147833073-e362f2be-3db7-41f8-aa05-6529aa169607.png" width="500" height="300">
    </td>
  </tr>  
</table>
<h3>Interactive graphs example :</h1>
      <img src="https://user-images.githubusercontent.com/84092005/148060571-67c39a84-3214-46e4-a810-ccbbda895693.png" >

### Conclusion :
After analyzing the data and removing the irrelevant columns, we tried different kind of machine learning models to see which one suits our dataset the best. In the end, we chose the **Random Forest** model which provides us the best results **(r²:0.87 / RMSE:222, corresponds to bikes)** on the test set, and that is why we take the decision to use this model in our flask API.

**Our model is efficient when predicting values under 1500**, when it is larger it doesn't match reality. This can be the result of a number of rows of high value (higher than 1500) not big enough to train the model correctly for huge values.

If we take the **MAE metric**, we can tell that our **predictions are correct with +-139 bikes of error**. This score is due to the errors caused by big value predictions.

In fact if we look at the dataset, we see that **75%** of the rows corresponds to values (number of bikes rented) **under 1085**. Which is **too narrowed**.

We trained our model on a dataset excluding values above 1500 and the MAE was about 60 (a difference avering 60 bikes per predictions), without losing too much of R2 square. It shows that if we had a dataset with more diversed values for the target, then we would surely have better results.

In conclusion our model is reliable when predicting values under 1500 bikes, but we would not recommand to trust it for huge values.


### Acknowledgement : 
in order to do this work we used some contents of shared ressources online that helped us figure out how to present our work.

we took some inspiration from these :
- for CSS :  https://freefrontend.com/css-forms/
- for API : https://towardsdatascience.com/how-to-easily-deploy-machine-learning-models-using-flask-b95af8fe34d4
- for Machine Learning : https://www.kaggle.com/faressayah/linear-regression-house-price-prediction

Thanks for reading !
