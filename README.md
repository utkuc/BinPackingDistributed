# Solving Bin Packing Problem on Distributed Systems
Simple Wikipedia Definition:

In the **bin packing problem**, items of different volumes must be packed into a finite number of bins or containers each of a fixed given volume in a way that minimizes the number of bins used. In [computational complexity theory](https://en.wikipedia.org/wiki/Computational_complexity_theory "Computational complexity theory"), it is a [combinatorial](https://en.wikipedia.org/wiki/Combinatorics "Combinatorics")  [NP-hard](https://en.wikipedia.org/wiki/NP-hard "NP-hard") problem. The [decision problem](https://en.wikipedia.org/wiki/Decision_problem "Decision problem") (deciding if items will fit into a specified number of bins) is [NP-complete](https://en.wikipedia.org/wiki/NP-complete "NP-complete").

We aproached this problem on distributed system with our own algortihm. Solving bin packing problem on big input data is challange and while doing so one must consider efficiency of algorthim and running time for the program.

While solving this problem on distributed systems we created an algorithm to minimize side effects of adding new worker nodes(computers) to the system.

### Requirements:
- Python 3.8+ : [Download](https://www.python.org/).
- pip Package Manager: [Download](https://pypi.org/project/pip/).
- Windows 8, 10 or Linux Based System
- Spark&Hadoop :  [Download](https://spark.apache.org/downloads.html).

# Installation


### Installing Packages:

- Make sure you have at least python 3.8 installed on your machine and your pip is up to date.

```python
	$ python --version
	$ pip --version
```
- Clone this project to somewhere on your system and create virtual environment. [How to create virtual environment](https://docs.python.org/3/tutorial/venv.html).
- After activating virtual environment, download and install packages from requirements.txt with:
```
	$ pip install -r requirements.txt
```
Installation for Spark on Windows can be found on this link: 
https://aamargajbhiye.medium.com/apache-spark-setup-a-multi-node-standalone-cluster-on-windows-63d413296971



#  Setting up Spark and Changing Connection Strings

Our implementation for algrotihm is located at **BinPackingDistributed**.py. You will need to configure connection string after setting up spark master node and worker nodes.
```python
spark = SparkSession.builder.appName("BinPackingDistributed ").master("spark://192.168.0.10:7077").getOrCreate()
```
Master function must take your spark url for master node. 

