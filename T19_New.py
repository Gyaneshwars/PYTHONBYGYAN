
import json
import time

with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8") as f:
  data = json.load(f)

extractedData=data['extractedData']
historicalData=data["historicalData"]
filingMetadata=data["filingMetadata"]


# associatedStrings=[{ "id": 21942, "group": 2, "includeFlag": 1, "keyword": ".*lend.*" },{ "id": 21940, "group": 1, "includeFlag": 1, "keyword": ".*loan.*" },{ "id": 21939, "group": 1, "includeFlag": 1, "keyword": ".*Securit.*" },{ "id": 21941, "group": 2, "includeFlag": 1, "keyword": ".*Securit.*" },{ "id": 34513, "group": 5, "includeFlag": 1, "keyword": ".*Securit.*Borrow.*" }]

# parameters = { "LHSTags": [ { "value": "DBSL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBSL" } ] }

# associatedStrings=[{ "id": 34512, "group": 5, "includeFlag": 1, "keyword": ".*doll.*roll.*" },{ "id": 21938, "group": 4, "includeFlag": 1, "keyword": ".*repo.*" },{ "id": 21933, "group": 1, "includeFlag": 1, "keyword": ".*Repurchase.*" }]

# parameters = { "LHSTags": [ { "value": "DBRP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBRP" } ] }

# associatedStrings=[{ "id": 21932, "group": 1, "includeFlag": 1, "keyword": ".*loan.*" },{ "id": 21931, "group": 1, "includeFlag": 1, "keyword": ".*Mortgage.*" }]

# parameters = { "LHSTags": [ { "value": "DBML" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBML" } ] }

# associatedStrings=[{ "id": 21929, "group": 1, "includeFlag": 1, "keyword": ".*Debent.*" }]

# parameters = { "LHSTags": [ { "value": "DBDB" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBDB" } ] }

# associatedStrings=[{ "id": 34511, "group": 7, "includeFlag": 1, "keyword": ".*Federal.*Home.*Loan.*" },{ "id": 34510, "group": 6, "includeFlag": 1, "keyword": ".*FHLB.*" }]

# parameters = { "LHSTags": [ { "value": "DBFH" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBFH" } ] }

# associatedStrings=[{"id": 36334,"group": 22,"includeFlag": 1,"keyword": ".*Banc.*"},{"id": 34591,"group": 21,"includeFlag": 1,"keyword": ".*Bank.*"},{"id": 34503,"group": 14,"includeFlag": 1,"keyword": ".*Bill.*Discount.*"},{"id": 34502,"group": 13,"includeFlag": 1,"keyword": ".*BNDES.*"},{"id": 38477,"group": 247,"includeFlag": 1,"keyword": ".*Buyers.*Credit.*"},{"id": 36335,"group": 23,"includeFlag": 1,"keyword": ".*HDFC.*"},{"id": 34508,"group": 19,"includeFlag": 1,"keyword": ".*ICBC.*"},{"id": 36336,"group": 24,"includeFlag": 1,"keyword": ".*ICICI.*"},{"id": 36337,"group": 25,"includeFlag": 1,"keyword": ".*IDB.*"},{"id": 36339,"group": 27,"includeFlag": 1,"keyword": ".*KFW.*"},{"id": 34505,"group": 16,"includeFlag": 1,"keyword": ".*Murabah.*"},{"id": 36338,"group": 26,"includeFlag": 1,"keyword": ".*Note.*Discounted.*"},{"id": 34509,"group": 20,"includeFlag": 1,"keyword": ".*OBC.*"},{"id": 34504,"group": 15,"includeFlag": 1,"keyword": ".*Packing.*Credit.*"},{"id": 36340,"group": 28,"includeFlag": 1,"keyword": ".*PNB.*"},{"id": 36341,"group": 29,"includeFlag": 1,"keyword": ".*SBH.*"},{"id": 34506,"group": 17,"includeFlag": 1,"keyword": ".*SBI.*"},{"id": 36342,"group": 30,"includeFlag": 1,"keyword": ".*SCB.*"},{"id": 38476,"group": 246,"includeFlag": 1,"keyword": ".*Trust.*receipt.*"}]

# parameters = { "LHSTags": [ { "value": "DBBL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBBL" } ] }

# associatedStrings=[{ "id": 36333, "group": 1, "includeFlag": 1, "keyword": ".*Capital.*" },{ "id": 34499, "group": 5, "includeFlag": 1, "keyword": ".*Trust.*" }]

# parameters = { "LHSTags": [ { "value": "DBTP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBTP" } ] }

# associatedStrings=[{ "id": 21894, "group": 1, "includeFlag": 1, "keyword": ".*Lease.*" },{ "id": 34498, "group": 2, "includeFlag": 1, "keyword": ".*Leasing.*" }]

# parameters = { "LHSTags": [ { "value": "DBCL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBCL" } ] }

# associatedStrings=[{ "id": 21887, "group": 1, "includeFlag": 1, "keyword": ".*Bond.*" },{ "id": 34497, "group": 11, "includeFlag": 1, "keyword": ".*Certificat.*" },{ "id": 36332, "group": 13, "includeFlag": 1, "keyword": ".*Hybrid.*" },{ "id": 34495, "group": 9, "includeFlag": 1, "keyword": ".*Listed.*Debt.*Secur.*" },{ "id": 21893, "group": 4, "includeFlag": 1, "keyword": ".*Loan.*stock.*" },{ "id": 36331, "group": 12, "includeFlag": 1, "keyword": ".*Negotiable.*" },{ "id": 34496, "group": 10, "includeFlag": 1, "keyword": ".*SUKUK.*" }]

# parameters = { "LHSTags": [ { "value": "DBBN" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBBN" } ] }

# associatedStrings=[{'id': 34483,'group': 10,'includeFlag': 1,'keyword': '.*Bridge.*Finance.*'},{'id': 21878,'group': 4,'includeFlag': 1,'keyword': '.*Credit.*'},{'id': 34490,'group': 17,'includeFlag': 1,'keyword': '.*Equipment.*Financ.*'},{'id': 34489,'group': 16,'includeFlag': 1,'keyword': '.*Export.*Financ.*'},{'id': 21880,'group': 5,'includeFlag': 1,'keyword': '.*Factoring.*'},{'id': 21886,'group': 8,'includeFlag': 1,'keyword': '.*Finan.*'},{'id': 34482,'group': 9,'includeFlag': 1,'keyword': '.*Financial.*Institution.*'},{'id': 34484,'group': 11,'includeFlag': 1,'keyword': '.*Government.*'},{'id': 34485,'group': 12,'includeFlag': 1,'keyword': '.*Govt.*'},{'id': 34488,'group': 15,'includeFlag': 1,'keyword': '.*Import.*Financ.*'},{'id': 21879,'group': 4,'includeFlag': 1,'keyword': '.*Insti.*'},{'id': 34486,'group': 13,'includeFlag': 1,'keyword': '.*Insurance.*Compan.*'},{'id': 21874,'group': 1,'includeFlag': 1,'keyword': '.*loan.*'},{'id': 34487,'group': 14,'includeFlag': 1,'keyword': '.*Project.*Financ.*'},{'id': 21885,'group': 8,'includeFlag': 1,'keyword': '.*Vendor.*'}]

# parameters = { "LHSTags": [ { "value": "DBTL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBTL 1 to N" } ] }

# associatedStrings=[{"id": 36330,"group": 20,"includeFlag": 1,"keyword": ".*Agreement.*"},{"id": 21871,"group": 12,"includeFlag": 1,"keyword": ".*capital.*"},{"id": 21873,"group": 3,"includeFlag": 1,"keyword": ".*capital.*"},{"id": 21869,"group": 11,"includeFlag": 1,"keyword": ".*card.*"},{"id": 21864,"group": 9,"includeFlag": 1,"keyword": ".*cash.*"},{"id": 21852,"group": 2,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21865,"group": 9,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21868,"group": 11,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21867,"group": 10,"includeFlag": 1,"keyword": ".*facilit.*"},{"id": 36329,"group": 19,"includeFlag": 1,"keyword": ".*Facilit.*"},{"id": 21863,"group": 8,"includeFlag": 1,"keyword": ".*financ.*"},{"id": 21860,"group": 7,"includeFlag": 1,"keyword": ".*floor.*"},{"id": 34477,"group": 16,"includeFlag": 1,"keyword": ".*Invoice.*Discount.*Facilit.*"},{"id": 21850,"group": 2,"includeFlag": 1,"keyword": ".*line.*"},{"id": 21851,"group": 2,"includeFlag": 1,"keyword": ".*of.*"},{"id": 21866,"group": 10,"includeFlag": 1,"keyword": ".*operating.*"},{"id": 21872,"group": 13,"includeFlag": 1,"keyword": ".*operating.*"},{"id": 21861,"group": 7,"includeFlag": 1,"keyword": ".*plan.*"},{"id": 34476,"group": 15,"includeFlag": 1,"keyword": ".*Receivable.*Facilit.*"},{"id": 21859,"group": 6,"includeFlag": 1,"keyword": ".*Revolv.*"},{"id": 21862,"group": 8,"includeFlag": 1,"keyword": ".*running.*"},{"id": 34475,"group": 14,"includeFlag": 1,"keyword": ".*Swing.*Line.*"},{"id": 21870,"group": 12,"includeFlag": 1,"keyword": ".*working.*"}]

# parameters = { "LHSTags": [ { "value": "DBRC" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBRC 1 to N" } ] }

# associatedStrings=[{"id": 21846,"group": 1,"includeFlag": 1,"keyword": ".*Commercial.*"},{"id": 34473,"group": 2,"includeFlag": 1,"keyword": ".*Market.*"},{"id": 34472,"group": 2,"includeFlag": 1,"keyword": ".*Money.*"},{"id": 21847,"group": 1,"includeFlag": 1,"keyword": ".*Paper.*"},{"id": 34474,"group": 2,"includeFlag": 1,"keyword": ".*Paper.*"}]

# parameters = { "LHSTags": [ { "value": "DBCP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBCP 1 to N" } ] }

# associatedStrings=[{"id": 34521,"group": 3,"includeFlag": 1,"keyword": ".*Adjust.*"},{"id": 34520,"group": 3,"includeFlag": 1,"keyword": ".*Derivativ.*"},{"id": 34519,"group": 3,"includeFlag": 1,"keyword": ".*Hedgin.*"}]

# parameters = { "LHSTags": [ { "value": "DBHAD" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBHAD" } ] }

# associatedStrings=[{"id": 36345,"group": 3,"includeFlag": 1,"keyword": ".*Asset.*backed.*securities.*"},{"id": 36346,"group": 4,"includeFlag": 1,"keyword": ".*Asset.*based.*securities.*"},{"id": 28016,"group": 2,"includeFlag": 1,"keyword": ".*securitis.*"},{"id": 28017,"group": 1,"includeFlag": 1,"keyword": ".*securitiz.*"}]

# parameters = { "LHSTags": [ { "value": "UDSF" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag UDSF" } ] }

# associatedStrings=[{"id": 36343,"group": 3,"includeFlag": 1,"keyword": ".*Asset.*backed.*securities.*"},{"id": 36344,"group": 4,"includeFlag": 1,"keyword": ".*Asset.*based.*securities.*"},{"id": 28014,"group": 2,"includeFlag": 1,"keyword": ".*securitis.*"},{"id": 28015,"group": 1,"includeFlag": 1,"keyword": ".*securitiz.*"}]

# parameters = { "LHSTags": [ { "value": "MCSF" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag MCSF" } ] }

# associatedStrings=[{"id": 34517,"group": 3,"includeFlag": 1,"keyword": ".*Asset.*Backed.*securities.*"},{"id": 34518,"group": 4,"includeFlag": 1,"keyword": ".*Asset.*Based.*securities.*"},{"id": 28012,"group": 2,"includeFlag": 1,"keyword": ".*securitis.*"},{"id": 28013,"group": 1,"includeFlag": 1,"keyword": ".*securitiz.*"}]

# parameters = { "LHSTags": [ { "value": "DBSF" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBSF" } ] }

# associatedStrings=[{"id": 34516,"group": 5,"includeFlag": 1,"keyword": ".*Current.*Accoun.*"},{"id": 34514,"group": 3,"includeFlag": 1,"keyword": ".*Over.*Draft.*"},{"id": 34515,"group": 4,"includeFlag": 1,"keyword": ".*Over.*Drawn.*"},{"id": 25569,"group": 1,"includeFlag": 1,"keyword": ".*Overdraft.*"}]

# parameters = { "LHSTags": [ { "value": "DBBO" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBBO" } ] }

# associatedStrings=[{"id": 21942,"group": 2,"includeFlag": 1,"keyword": ".*lend.*"},{"id": 21940,"group": 1,"includeFlag": 1,"keyword": ".*loan.*"},{"id": 21939,"group": 1,"includeFlag": 1,"keyword": ".*Securit.*"},{"id": 21941,"group": 2,"includeFlag": 1,"keyword": ".*Securit.*"},{"id": 34513,"group": 5,"includeFlag": 1,"keyword": ".*Securit.*Borrow.*"}]

# parameters = { "LHSTags": [ { "value": "DBSL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBSL" } ] }

# associatedStrings=[{"id": 34512,"group": 5,"includeFlag": 1,"keyword": ".*doll.*roll.*"},{"id": 21938,"group": 4,"includeFlag": 1,"keyword": ".*repo.*"},{"id": 21933,"group": 1,"includeFlag": 1,"keyword": ".*Repurchase.*"}]

# parameters = { "LHSTags": [ { "value": "DBRP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBRP" } ] }

# associatedStrings=[{"id": 21932,"group": 1,"includeFlag": 1,"keyword": ".*loan.*"},{"id": 21931,"group": 1,"includeFlag": 1,"keyword": ".*Mortgage.*"}]

# parameters = { "LHSTags": [ { "value": "DBML" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBML" } ] }

# associatedStrings=[{"id": 21930,"group": 1,"includeFlag": 1,"keyword": ".*Mortgage.*"}]

# parameters = { "LHSTags": [ { "value": "DBMN" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBMN" } ] }

#associatedStrings=[{"id": 21929,"group": 1,"includeFlag": 1,"keyword": ".*Debent.*"}]

# parameters = { "LHSTags": [ { "value": "DBDB" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBDB" } ] }

# associatedStrings=[{"id": 34511,"group": 7,"includeFlag": 1,"keyword": ".*Federal.*Home.*Loan.*"},{"id": 34510,"group": 6,"includeFlag": 1,"keyword": ".*FHLB.*"}]

# parameters = { "LHSTags": [ { "value": "DBFH" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBFH 1 to N" } ] }

# associatedStrings=[{"id": 36334,"group": 22,"includeFlag": 1,"keyword": ".*Banc.*"},{"id": 34591,"group": 21,"includeFlag": 1,"keyword": ".*Bank.*"},{"id": 34503,"group": 14,"includeFlag": 1,"keyword": ".*Bill.*Discount.*"},{"id": 34502,"group": 13,"includeFlag": 1,"keyword": ".*BNDES.*"},{"id": 38477,"group": 247,"includeFlag": 1,"keyword": ".*Buyers.*Credit.*"},{"id": 36335,"group": 23,"includeFlag": 1,"keyword": ".*HDFC.*"},{"id": 34508,"group": 19,"includeFlag": 1,"keyword": ".*ICBC.*"},{"id": 36336,"group": 24,"includeFlag": 1,"keyword": ".*ICICI.*"},{"id": 36337,"group": 25,"includeFlag": 1,"keyword": ".*IDB.*"},{"id": 36339,"group": 27,"includeFlag": 1,"keyword": ".*KFW.*"},{"id": 34505,"group": 16,"includeFlag": 1,"keyword": ".*Murabah.*"},{"id": 36338,"group": 26,"includeFlag": 1,"keyword": ".*Note.*Discounted.*"},{"id": 34509,"group": 20,"includeFlag": 1,
# "keyword": ".*OBC.*"},{"id": 34504,"group": 15,"includeFlag": 1,"keyword": ".*Packing.*Credit.*"},{"id": 36340,"group": 28,"includeFlag": 1,"keyword": ".*PNB.*"},{"id": 36341,
# "group": 29,"includeFlag": 1,"keyword": ".*SBH.*"},{"id": 34506,"group": 17,"includeFlag": 1,"keyword": ".*SBI.*"},{"id": 36342,"group": 30,"includeFlag": 1,"keyword": ".*SCB.*"
# },{"id": 38476,"group": 246,"includeFlag": 1,"keyword": ".*Trust.*receipt.*"}]

# parameters = { "LHSTags": [ { "value": "DBBL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBBL" } ] }

# associatedStrings=[{"id": 36333,"group": 1,"includeFlag": 1,"keyword": ".*Capital.*"},{"id": 34499,"group": 5,"includeFlag": 1,"keyword": ".*Trust.*"}]

# parameters = { "LHSTags": [ { "value": "DBTP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBTP" } ] }

# associatedStrings=[{"id": 21894,"group": 1,"includeFlag": 1,"keyword": ".*Lease.*"},{"id": 34499,"group": 5,"includeFlag": 1,"keyword": ".*Leasing.*"}]

# parameters = { "LHSTags": [ { "value": "DBCL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBCL" } ] }

# associatedStrings=[{"id": 21887,"group": 1,"includeFlag": 1,"keyword": ".*Bond.*"},{"id": 34497,"group": 11,"includeFlag": 1,"keyword": ".*Certificat.*"},{"id": 36332,"group": 13,"includeFlag": 1,"keyword": ".*Hybrid.*"},{"id": 34495,"group": 9,"includeFlag": 1,"keyword": ".*Listed.*Debt.*Secur.*"},{"id": 21893,"group": 4,"includeFlag": 1,"keyword": ".*Loan.*stock.*"},{"id": 36331,"group": 12,"includeFlag": 1,"keyword": ".*Negotiable.*"},{"id": 34496,"group": 10,"includeFlag": 1,"keyword": ".*SUKUK.*"}]

# parameters = { "LHSTags": [ { "value": "DBBN" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBBN" } ] }

# associatedStrings=[{'id': 34483,'group': 10,'includeFlag': 1,'keyword': '.*Bridge.*Finance.*'},{'id': 21878,'group': 4,'includeFlag': 1,'keyword': '.*Credit.*'},{'id': 34490,'group': 17,'includeFlag': 1,'keyword': '.*Equipment.*Financ.*'},{'id': 34489,'group': 16,'includeFlag': 1,'keyword': '.*Export.*Financ.*'},{'id': 21880,'group': 5,'includeFlag': 1,
# 'keyword': '.*Factoring.*'},{'id': 21886,'group': 8,'includeFlag': 1,'keyword': '.*Finan.*'},{'id': 34482,'group': 9,'includeFlag': 1,'keyword': '.*Financial.*Institution.*'},{'id': 34484,'group': 11,'includeFlag': 1,'keyword': '.*Government.*'},{'id': 34485,'group': 12,'includeFlag': 1,'keyword': '.*Govt.*'},{'id': 34488,'group': 15,'includeFlag': 1,'keyword': '.*Import.*Financ.*'},{'id': 21879,'group': 4,'includeFlag': 1,'keyword': '.*Insti.*'},{'id': 34486,'group': 13,'includeFlag': 1,'keyword': '.*Insurance.*Compan.*'},{'id': 21874,'group': 1,'includeFlag': 1,'keyword': '.*loan.*'},{'id': 34487,'group': 14,'includeFlag': 1,'keyword': '.*Project.*Financ.*'},{'id': 21885,'group': 8,'includeFlag': 1,
# 'keyword': '.*Vendor.*'}]

# parameters = { "LHSTags": [ { "value": "DBTL" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBTL 1 to N" } ] }

# associatedStrings=[{"id": 36330,"group": 20,"includeFlag": 1,"keyword": ".*Agreement.*"},{"id": 21871,"group": 12,"includeFlag": 1,"keyword": ".*capital.*"},{"id": 21873,"group": 3,
# "includeFlag": 1,"keyword": ".*capital.*"},{"id": 21869,"group": 11,"includeFlag": 1,"keyword": ".*card.*"},{"id": 21864,"group": 9,"includeFlag": 1,"keyword": ".*cash.*"},{"id": 1852,"group": 2,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21865,"group": 9,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21868,"group": 11,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 21867,"group": 10,"includeFlag": 1,"keyword": ".*facilit.*"},{"id": 36329,"group": 19,"includeFlag": 1,"keyword": ".*Facilit.*"},{"id": 21863,"group": 8,"includeFlag": 1,"keyword": ".*financ.*"},{"id": 21860,"group": 7,"includeFlag": 1,"keyword": ".*floor.*"},{"id": 34477,"group": 16,"includeFlag": 1,"keyword": ".*Invoice.*Discount.*Facilit.*"},{"id": 21850,"group": 2,"includeFlag": 1,"keyword": ".*line.*"},{"id": 21851,"group": 2,"includeFlag": 1,"keyword": ".*of.*"},{"id": 21866,"group": 10,"includeFlag": 1,
# "keyword": ".*operating.*"},{"id": 21872,"group": 13,"includeFlag": 1,"keyword": ".*operating.*"},{"id": 21861,"group": 7,"includeFlag": 1,"keyword": ".*plan.*"},{"id": 34476,"group": 15,"includeFlag": 1,"keyword": ".*Receivable.*Facilit.*"},{"id": 21859,"group": 6,"includeFlag": 1,"keyword": ".*Revolv.*"},{"id": 21862,"group": 8,"includeFlag": 1,"keyword": ".*running.*"},{"id": 34475,"group": 14,"includeFlag": 1,"keyword": ".*Swing.*Line.*"},{"id": 21870,"group": 12,"includeFlag": 1,"keyword": ".*working.*"}]

# parameters = { "LHSTags": [ { "value": "DBRC" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBRC 1 to N" } ] }

# associatedStrings=[{"id":21846,"group":1,"includeFlag":1,"keyword":".*Commercial.*"},{"id":34473,"group":2,"includeFlag":1,"keyword":".*Market.*"},{"id":34472,"group":2,"includeFlag":1,"keyword":".*Money.*"},{"id":21847,"group":1,"includeFlag":1,"keyword":".*Paper.*"},{"id":34474,"group":2,"includeFlag":1,"keyword":".*Paper.*"}]

# parameters = { "LHSTags": [ { "value": "DBCP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBCP 1 to N" } ] }

# associatedStrings=[{"id":34521,"group":3,"includeFlag":1,"keyword":".*Adjust.*"},{"id":34520,"group":3,"includeFlag":1,"keyword":".*Derivativ.*"},{"id":34519,"group":3,"includeFlag":1,"keyword":".*Hedgin.*"}]

# parameters = { "LHSTags": [ { "value": "DBHAD" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag DBHAD" } ] }

associatedStrings=[{"id":36345,"group":3,"includeFlag":1,"keyword":".*Asset.*backed.*securities.*"},{"id":36346,"group":4,"includeFlag":1,"keyword":".*Asset.*based.*securities.*"},{"id":28016,"group":2,"includeFlag":1,"keyword":".*securitis.*"},{"id":28017,"group":1,"includeFlag":1,"keyword":".*securitiz.*"}]

parameters = { "LHSTags": [ { "value": "UDSF" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Specified strings should exist in line item description for the tag UDSF" } ] }


def T19(historicalData,filingMetadata,extractedData,associatedStrings,parameters):

    import datetime
    #reading parameter values
    def get_param_value(parameters,key):
        try:
            param_value=[]
            for pam in parameters[key]:
                param_value.append(pam.get('value'))
        except:
            param_value=''
        return param_value
    def get_param_id(parameters,key):
        try:
            param_id=[]
            for pam in parameters[key]:
                param_id.append(pam.get('value'))
                
        except:
            param_id=''
        return param_id 
 
    
    industry_inc=get_param_value(parameters,'INDUSTRY_INCLUDE')
    industry_exc=get_param_value(parameters,'INDUSTRY_EXCLUDE')
    template=get_param_value(parameters,'template')
    gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
    gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
    country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
    country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
    AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
    PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
    Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
    SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
    lhs_tag=parameters['LHSTags'][0]['value'].split(',')
    lhs_tags=[]
    for tg in lhs_tag:
        lhs_tags.append(str(tg))
        lhs_tags.append(str(tg)+'#CV')
        lhs_tags.append(str(tg)+'#IL')
    MBSflag=get_param_value(parameters,'Main/Breakup/All')
    if MBSflag=="":
        MBSflag='All'
    stmt=parameters['StatementType'][0]['value']
    # if 'FastIncomeStatement' in filingMetadata['sections'].keys():
    #     stmt='FastIncomeStatement'
    # else:
    #     stmt='IncomeStatement'
    
    if AsReportedPeriodEndDate!="":
        ARoperator=AsReportedPeriodEndDate[0]
        AsReportedPeriodEndDate=AsReportedPeriodEndDate[1:]
   
    
    def check_period(ARoperator):
        if ARoperator=='>':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')>datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        elif ARoperator=='<':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')<datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        elif ARoperator=='=':
            ret=datetime.datetime.strptime(filingMetadata['metadata']['periodEndDate'],'%Y-%m-%d')==datetime.datetime.strptime(AsReportedPeriodEndDate,'%Y-%m-%d')
        else:
            ret='True'
        return ret
    
    results=[]
    tag=get_param_value(parameters,'LHSTags')
    #print(tag)
    filingid=filingMetadata['metadata']['filingId']
    #validating parameter values
    
    if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
    and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
    and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
    and (len(country_inc)==0 or filingMetadata["metadata"]["classification"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["classification"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys()))))\
    and len(tag)!=0:
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\	
    
        import re

        inc={}
        exc={}
        for st in associatedStrings:
            if st['includeFlag']==1:
                if st['group'] in inc.keys():
                    inc[st['group']].append(st['keyword'])
                else:
                    inc[st['group']]=[st['keyword']]
            if st['includeFlag']==0:
                if st['group'] in exc.keys():
                    exc[st['group']].append(st['keyword'])
                else:
                    exc[st['group']]=[st['keyword']]
                
        #print(inc)     #Reading associatedStrings
        #print(inc.keys())  #Reading & fetching groupid
        #print(exc)
    line_items=[]
    high1=[]


        
    for key in extractedData.keys():
        for ins in extractedData[key].keys():
            if extractedData[key][ins]['section']==stmt:
                if 'childRows' in extractedData[key][ins].keys() and ('Breakup Only' in MBSflag or 'All' in MBSflag):
                    for chins in extractedData[key][ins]['childRows'].keys():
                        if extractedData[key][ins]['childRows'][chins]['tag'] in lhs_tags:
                            line_items.append(extractedData[key][ins]['childRows'][chins]['description'])
                            high1.append({'tag':extractedData[key][ins]['childRows'][chins]['tag'],'description':extractedData[key][ins]['childRows'][chins]['description'],'ins':chins})

                if 'childRows' not in extractedData[key][ins].keys() and ('Main Only' in MBSflag or 'All' in MBSflag) and extractedData[key][ins]['isChildRow'] is False:
                    if extractedData[key][ins]['tag'] in lhs_tags:
                        line_items.append(extractedData[key][ins]['description'])
                        high1.append({'tag':extractedData[key][ins]['tag'],'description':extractedData[key][ins]['description'],'ins':ins})

    print(line_items)    #Fetching Json discription

    line_items_inc_match=[]
    for line in line_items:
        print(line)
        print(inc)
        print(inc.keys())
        for st in inc.keys():
            print(st)
            print(inc[st])
            t=len(inc[st])
            print(t)
            i=0
            for s in inc[st]:
                if re.search(s.lower(),line.lower()):
                    i=i+1
            if i!=0 and i==t:
                line_items_inc_match.append(line)
    print(line_items_inc_match)  
    print(len(line_items_inc_match))      
    if len(line_items_inc_match)>0 and len(line_items)>0:
        for l in line_items_inc_match:
            if l in line_items:
                line_items.remove(l)
                       
    line_items_exc_match=[]
    for line in line_items:
        for st in exc.keys():
            t=len(exc[st])
            i=0
            for s in exc[st]:
                if re.search(s.lower(),line.lower()):
                    i=i+1
            if i!=0 and i==t:
                line_items_exc_match.append(line)
    mat=set(line_items_exc_match)
    #print(mat)
    if len(line_items_exc_match)>0 and len(line_items)>0:
        for l in line_items_exc_match:
            if l in line_items:
                line_items.remove(l)
    high=[]
    CTC=[]
    # print(high1)
    for h in high1:
        # print(h)
        # print(line_items)
        # print(h['description'])
        if h['description'] in line_items:
            high.append({"section": stmt, "row": {"name": h['tag'], "id": h['ins']}, "filingId": int(filingMetadata['metadata']['filingId'])})
            CTC.append({"statement": stmt, "tag": h['tag'], "description": h['description'],"filingId": filingid})
        # print(high)
        # print(CTC)
        if len(high)>0:
            err="Specified strings should exist in line item description for the tags " + str(lhs_tag) 
            result={"highlights": [],"error":""}
            result["highlights"]=high     
            result["error"] = err
            result["checkGeneratedFor"]={"statement": stmt, "tag": str(lhs_tag), "description": "", "refFilingId": "","filingId": int(filingid),"objectId": "", "peo": "","fpo": "", "diff": ""}
            result["checkGeneratedForList"]=CTC
            results.append(result)
    # err=str(line_items)
    # result={"highlights": [],"error":""}

    
    
    return results 


if __name__ == '__main__':
  start_time = time.time()
  results1 = T19(historicalData,filingMetadata,extractedData,associatedStrings,parameters)
  print(results1)
  print('Total time: ', time.time() - start_time)
    
 
