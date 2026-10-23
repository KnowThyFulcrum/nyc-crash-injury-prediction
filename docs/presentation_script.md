### **Slide 1 – Title**
Hi everyone. The title of my project is “Comparing Two Machine Learning Tools for Predicting Injuries in New York City Car Crashes.” 

---

### **Slide 2 – The Problem & The Goal**
First, the problem.  

Car crashes are a serious public safety issue. If we can predict which crashes are more likely to cause injuries, it can help emergency services, city planners, and policy makers.  

My goal was simple: take the same large real-world dataset and compare two popular machine learning tools — PySpark MLlib and Scikit-Learn — to see which one works better for predicting injuries.

---

### **Slide 3 – The Dataset**
I used one million cleaned crash records from New York City Open Data.  

The question I asked the models was: “Did this crash cause an injury — yes or no?”  
About 25% of the crashes did cause an injury. This means the data is imbalanced — there are many more “no injury” cases than “injury” cases.

The features I used were things like location, time of day, what caused the crash, and the type of vehicles involved.  

One very important rule: I removed the columns that already counted how many people were injured. If I had left those in, the models could have just “cheated” by looking at the answer. This is called data leakage, and I made sure to avoid it.

---

### **Slide 4 – What Was Compared**
I compared two tools.  

On the left is **PySpark MLlib**.  
This is a distributed tool. It’s built for very large data and can run on many computers at the same time. I was able to train it on the full one million rows. It’s better when you’re doing production-scale work.  

On the right is **Scikit-Learn**.  
This is a single-computer tool. It’s very popular and easy to use in Python, but it works best when the data fits in the computer’s memory. Because of that, I used a smaller sample of about 60,000 rows. It’s great for quick experiments.

---

### **Slide 5 – The Two Models That Were Tested**
I tested the same two models in both tools.  

The first model is **Logistic Regression**.  
It’s a simple and fast model. It basically draws a straight line to separate injury crashes from non-injury crashes. It’s easy to understand and usually a strong starting point.  

The second model is **Random Forest**.  
This one is more complex. It’s made of many decision trees working together. It can capture more complicated patterns, but it takes longer to train.

---

### **Slide 6 – Results at a Glance**
Here’s a quick look at the results.  

Both tools scored almost the same on AUC, which measures how well the model ranks risk. Higher AUC and F1 are better.  

You’ll notice that Logistic Regression did well in both tools, while Random Forest was a bit weaker, especially in Scikit-Learn.

---

### **Slide 7 – Key Findings**
Here are the four main things I found.  

**First**, both tools had very similar ranking ability. Their AUC scores were almost the same — between 0.74 and 0.76.  

**Second**, Logistic Regression won. The simpler model beat Random Forest on most metrics and trained much faster.  

**Third**, PySpark handles scale better. It trained on the full one million rows, while Scikit-Learn needed a smaller sample to avoid running out of memory.  

**Fourth**, class imbalance matters. Only about 25% of the crashes caused injuries. Because of that, Scikit-Learn’s F1 and Recall scores dropped more.  

I want to be clear about one thing: I did not balance the data. I left it the way it really is, which shows that only about 25% of the crashes caused injuries. I could have given the injury cases extra weight so the model pays more attention to them, but I didn’t do that. I wanted to see how the models would do with the real numbers. That’s why Scikit-Learn’s F1-score and Recall were lower. The models mostly predicted “no injury” because that was the more common answer.

---

### **Slide 8 – When to Use Each Tool**
So when should you use each tool?  

**Use PySpark MLlib when:**  
- Your data is very large — hundreds of thousands or millions of rows  
- You need to train on the full dataset  
- You’re building something for production  
- You already have a Spark or cloud cluster  

**Use Scikit-Learn when:**  
- Your data fits comfortably in one computer’s memory  
- You want to experiment quickly  
- You’re prototyping or exploring ideas  
- You prefer simpler Python code  

---

### **Slide 9 – Conclusion**
To wrap up: both tools can solve this problem well.  

After cleaning one million New York City crash records and carefully making sure the models couldn’t cheat, I found that PySpark MLlib and Scikit-Learn produced almost the same ranking quality.  

Logistic Regression was the stronger and faster model overall.  
PySpark handled the full large dataset better, while Scikit-Learn was quicker on a smaller sample.  

One thing I would try in the future is adding weights to see if that improves the F1-score and Recall, especially for the injury class.  

In the end, the best tool depends on the size of your data and your goals.

---

### **Slide 10 – Thank You**
Thank you for listening.  