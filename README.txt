This lab was broken into 3 parts.

TASK 1:
For task 1, I started with Task1a.java and performed a reduce side join between the countries csv file
and the access log files. The token that matches us is the hostname, so I extracted the 0th index.
I added an A and and B to the access log and countries respecitively, so my reducer would be able to
see which is which later on. I chose a reduce side join because you can combine two large sources which
is a big advantage, especially if these files were much larger where a Map-Side Join might struggle.

In Task1b.java, I have a simple counter that goes through each occurance and sums up the number of "1"s it sees
in the reducer. Now that I have counts for everything, it's time to sort. Sorting occurs in Task1c.java
where I had to get a little crafting. Because the map wants to sort in ascending order, I mulitplied
the count (which is passed in as our key) by -1, so the largest negative would be written first when we 
reduce. Just before it writes the count, we multiply it again so we have the correct count in the correct,
descending order.


TASK 2:
I started in task2a.java and did another reduce side join to bring in the entire log file (which I should have
done for task 1). I went through the same process but instead this time now kept tract of the country,
url, and the count. I created a new class called CountryCount so I could do a secondary sort of the counts.
The first way this program sorts is my key in descending order, which is perfect. But from there,
we need to interate through the different urls, and we again multiply the count by -1 in the comparator
to get the descending order for the desired output.


TASK 3:
