# CodersLink Test


#Running App
Follow next steps to run app
- Uncompress de *.zip
- Run env with .\env\Scripts\activate:
  - (env) PS D:\MIGUEL\Miguel\MIGUEL\2022\FullStack\test\coderslink\src> .\env\Scripts\activate
- Run app
  - py app.py
-App excute all steps:
    -(env) PS D:\MIGUEL\Miguel\MIGUEL\2022\FullStack\test\coderslink\src> py app.py   
    -<----CODERSLINK Test---->
    - 1 CleanedTransactions -OK
    - 2a.- Report TransactionsByDay -OK
    - 2b.- Report CustomersTotalsByDay -OK
    - 3.- SQLite -OK
    - 4.- SQL Files -OK

-The sources and output  are in the path:

    /data
        /input
        /output
            /db
                database_for_future.sqlite   SQLite DB
            /query
                CustomerTotalsByDay.sql  File with the query to obtain customers totals by day
                TransactionsByDay.sql File with the query to obtain transactions by day
            /reports
                TransactionsByDay.csv Report 1 with groupby transactions by day
                CustomerTotalsByDay.csv Report 2 with groupby customers totals by day
        CleanedTransactions.csv  File without duplicates records based in TransactionID
    .env  The app use global variables environment
    /src
        app.py   code files refactored
        appnorecfactored.py  code file without refactored

