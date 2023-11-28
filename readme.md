## Chess in Catalonia

Chess.com offers a public API where we can access several endpoints to collect usage and results data about the chess games played.

Since it is a very large repository, Chess.com limits its data to 10000 players. For this reason, I have focused the project on players who have indicated their country/region as Catalonia.

The process for obtaining and adjusting the data has been the following:

- Selection of the necessary endpoints
- API calls from Python to obtain a set of dataframes.
- The fetching process has been intense since some of the extractions took more than 40 minutes due to the amount of data delivered.
- Data cleaning: columns, nulls, date fields,...

In addition to the data, PGN files have been obtained, detailing the progress and result of the game through metadata and notation of the moves. We obtained about 3600 PGN files belonging to Catalunya.

### SQL

Subsequently the CSV files have been processed in MYSQL, where a collection of queries have been generated to obtain an output of conclusions such as: number of players, openings, levels, .....

### Visualization (Tableau)

Using the SQL queries built, in some cases we have replicated the behavior in Tableau by calculated fields and in other cases, we have obtained CSV outputs to be imported into Tableau.

!https://public.tableau.com/app/profile/pere.martin.moraleja/viz/ProjectIV-CHESS/ChessAnalysis?publish=yes


### Results

Finally, a Story is built in Tableau with dashboards to visually represent the conclusions drawn from the data obtained in Chess.com.


**NOTE: Thanks to Chess.com for offering a spectacular API.**






