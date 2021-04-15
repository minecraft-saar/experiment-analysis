# experiment-analysis

This program analyzes experiment data for our [minecraft-saar](https://minecraft-saar.github.io/)
experiments.  As we need to extract information specific to the experiment setup,
some parts need work to generalize (we e.g. currently hardcode information
about what is being built).

Before you can run an analysis, you have to import the game data from
the experiment server onto your own mariadb server (unless you run
this analysis directly on the experiment server, which I would not do
as it can alter the data).

You can dump the database like this (change user & db name as needed):

`mysqldump --add-drop-table EXPERIMENTDATABASE > EXPERIMENTDATABASE.sql`

and then import it locally:

`mariadb -user minecraft < EXPERIMENTDATABASE.sql`


You need to configure the program using a yaml file, an example is
`example-config.yml`. Configure your database connection and output
directory there.

Configure database connection and output directory in `config.yml`.
There is an example configuration in `example-config.yml`.

Get evaluation in separate Markdown files with `./gradlew run --args="<arguments>"`.

## Arguments
`--allGamesAnalysis`:  Runs a game analysis for every game in the database

`--architectAnalysis`: Runs an aggregate analysis for each architect in the database

`--fullAnalysis`: Runs the entire experiment analysis which includes
                              aggregate analyses per scenario, architect and
                              every scenario-architect combination, an analysis
                              for each game and a csv file

`-h, --help`: Show a help message and exit.

`--partialAnalysis=<scenario> <architect> <only successful (true/false)>`:
Runs an aggregate analysis for games with the specified properties

`--saveCSV`: Creates an overview csv file. 
This csv file is also the input for the R-notebook `analysis.rmd`

`--scenarioAnalysis`: Runs an aggregate analysis for each scenario in the
                              database

`--singleGameAnalysis=<gameId>`: Runs the analysis for the game with the given game id

`-V, --version`: Print version information and exit.

## Evaluation files

### data.csv
The first lines consist of a numbered list of questions.
Then follows a table with the following columns:

- gameid
- scenario
- architect
- wasSuccessful
- timeToSuccess
- numBlocksPlaced
- numBlocksDestroyed
- numMistakes
- HLO0 (time needed to complete high-level object 1)
- ...
- HL07
- Question0
- ...
- QuestionN:

If the time for a high-level object is negative, 
it was built before the previous object.

### Game analysis markdown files
 - Connection from: client IP address
 - Player name
 - Scenario
 - Architect
 - Successful: true/false
 - Start Time: The first time stamp of the game, approximately when the player
 logged in
 - Success Time
 - End Time: The last time stamp of the game, approximately when the player 
 logged out or was kicked
 - Experiment Duration: Time between start time and success time
 - Total time logged in: Time between start time and end time
 - Number of blocks placed
 - Number of blocks destroyed
 - Number of mistakes: Number of times the architect requests removal
 - Duration per block: times in ms between each BlockPlacedEvent (regardless of 
 the instructions)
 - Durations per High-level object (HLO): For each HLO the time the user needed
  to build it. The duration for each HLO begins when the previous HLO was completed
  (with the welcome message for the first HLO) and ends when all blocks of the HLO
  are present, regardless of the instructions. If HLOs were built in a wrong order,
  durations can be negative.
 - Durations per Instruction: *implementation not finished*
 - Durations per Block Instruction: *implementation not finished*

### Aggregate analysis Markdown files
 - Number of games (with this filter)
 - Average game duration
 - Fraction of successful games
 - Fraction of players making a mistake
 - Average number of mistakes
 - Average number of blocks placed
 - Average number of blocks destroyed
 - Games in this category: a list of game ids
 - Likert Questions: Table with question, mean, standard deviation, Median, minimum and maximum
 - Free-form Questions: list of answers for every free-form question
 - Average Duration per HLO: list of high-level objects in this scenario with
  their average building duration. Only for aggregate files where every game 
  is from the same scenario.


