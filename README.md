# Analyzing pittags' logs

During my Ph.D. research, I have tracked animals using pittags loggers technology. To study how they change their activity at different environmental, I also recorded their body temperature (using implanted transmitters) and the ambient temperature (using ibuttons). 

A major challenge in the project was to deal with the different resources of the data. For the pittags data, for example, I wanted to know what was the ambient and body temperature at each activity log. Basically, for every activity log, I needed to search the body and ambient temperature logs for the specific animal, enclosure, habitat, and time. To achieve my goal., I entered all the logs (see txt files for sample of the data) to a MySQL database, and developed an SQL procedure to find the closest temperature logs and add them to the pittags SQL table (see connect_tables.sql file). 

Another major challenge was to filter the noise in the body temperature data. Since the data was collected using a transmitter/receiver system, the receiver often got unreasonable data, especially when an animal was far from the listening antennas. To filter the data, I first entered all the data to the SQL database, and then I used R to filter the data into another table. 
