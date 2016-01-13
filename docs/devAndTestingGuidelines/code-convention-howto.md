#Code Convention#

1. Follow the code conventions of the language you are using.

2. Configure your IDE properly for indentation.  Scribengin uses 2 spaces as an indent.

3. Give classes, methods, and variables meaningful names.

#Committing Code#

1. Before checking in code, make sure your code causes no new failures.  This will require working knowledge of the state of the system and any current bugs.

2. No partial commits.

3. Always run ```git status``` to make sure you don't omit any resources or add anything unintentionally.

4. Resolve merge conflicts intelligently and retest.


#Development Guidelines#

1.  Classify and catagorize new code using different categories like util, core, service, debug, ui, etc.

2. When committing code that modifies something critical (i.e. core functionality), it is best to discuss with others on the team.  There may be a reason why a particular feature or functionality is not currently exposed.

3. Before modifying code, you should know the status of testing the project -  how many tests pass, how many tests fail, what exceptions are thrown, etc.  The effects of your code changes will be more apparent from this preliminary testing.

4. When you encounter a problem and require assistance, succinctly collect all relevant information such as log messages, exceptions, and test scenario. 


