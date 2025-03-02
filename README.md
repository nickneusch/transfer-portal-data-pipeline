# transfer-portal-data-pipeline

(Data is not provided for privacy reasons)

I developed a data pipeline for the Duquesne University Basketball coaching staff to keep track of how their potential transfer portal targets are playing. The data is periodically updated with their current statistics using a python script that web-scrapes it.

## Motivation

The Duquesne Basketball coaching staff was looking to keep track of specific players that they may want to target in the transfer portal next offseason. They gave me a spreadsheet with various names from each conference and asked if I was able to "update the spreadsheet" so they could see each of the players' respective stats. This gave me the idea to web-scrape statistics for players from each conference and left-join this with the potential targets spreadsheet I was given. Now, every couple days I run the code which outputs a full spreadsheet with the potential targets updated stats in seconds to send to the coaching staff and keep them updated.

## Workflow
1. Given spreadsheet of potential targets for the transfer portal next offseason.
2. Create a python script to web-scrape NCAA basketball player statistics
3. Left-join the web-scraped statistics with the given spreadsheet (by Name) ensuring that only desired players are included.
4. Output the spreadsheet.
5. Run the python script every couple days, updating the spreadsheet with the most recent statistics.
6. Send the spreadsheet to coaching staff.

##

Created in February 2025
