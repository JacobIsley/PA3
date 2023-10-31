Profiles:
A1: Without Taxation
A2: Without Taxation, Wikipedia Bomb
B1: With Taxation
B2: With Taxation, Wikipedia Bomb

Run PA3:
spark-submit --class pa3.WikipediaPageRank --master spark://charleston.cs.colostate.edu:30335 ~/PA3/jars/NAME_HERE.jar PROFILE /PA3/input/links-simple-sorted.txt /PA3/input/titles-sorted.txt /PA3/output

Run PA3 with small sample:
spark-submit --class pa3.WikipediaPageRank --master spark://charleston.cs.colostate.edu:30335 ~/PA3/jars/NAME_HERE.jar PROFILE /PA3/input/links-sample.txt /PA3/input/titles-sample.txt /PA3/outputSample
