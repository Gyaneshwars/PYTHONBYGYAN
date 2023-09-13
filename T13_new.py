
import json
import time

with open("C:\\Users\\gsravane\\Downloads\\T13 (1).json",encoding="utf8") as f:
  data = json.load(f)

extractedData=data['extractedData']
historicalData=data["historicalData"]
filingMetadata=data["filingMetadata"]

#includeFlag:0 ---Exclude     includeFlag:1 ---Include(string should exist)

# associatedStrings=[{ "id": 34010, "group": 4, "includeFlag": 0, "keyword": ".*bond.*" },{ "id": 34007, "group": 2, "includeFlag": 0, "keyword": ".*debentur.*" },{ "id": 34009, "group": 3, "includeFlag": 0, "keyword": ".*payable.*" },{ "id": 34008, "group": 3, "includeFlag": 0, "keyword": ".*note.*" }, { "id": 22435, "group": 1, "includeFlag": 1, "keyword": ".*Premium.*" }, { "id": 34011, "group": 5, "includeFlag": 1, "keyword": ".*premium.*" }, { "id": 34012, "group": 5, "includeFlag": 1, "keyword": ".*unamortise.*" },{ "id": 22434, "group": 1, "includeFlag": 1, "keyword": ".*Unamortized.*" }]

# parameters = { "LHSTags": [ { "value": "DBUP" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBUP but tag given is other than DBUP" } ] }

# associatedStrings=[{ "id": 34003, "group": 4, "includeFlag": 0, "keyword": ".*bond.*" },{ "id": 34004, "group": 5, "includeFlag": 0, "keyword": ".*commer.*paper.*" },{ "id": 34000, "group": 2, "includeFlag": 0, "keyword": ".*debentur.*" },{ "id": 34001, "group": 3, "includeFlag": 0, "keyword": ".*note.*" },{ "id": 34002, "group": 3, "includeFlag": 0, "keyword": ".*payable.*" }, { "id": 22433, "group": 1, "includeFlag": 1, "keyword": ".*discoun.*" }, { "id": 34005, "group": 6, "includeFlag": 1, "keyword": ".*discoun.*" }, { "id": 34006, "group": 6, "includeFlag": 1, "keyword": ".*unamortise.*" },{ "id": 22432, "group": 1, "includeFlag": 1, "keyword": ".*Unamortized.*" }]

# parameters = { "LHSTags": [ { "value": "DBUD" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBUD but tag given is other than DBUD" } ] }

# associatedStrings=[{ "id": 22414, "group": 1, "includeFlag": 1, "keyword": ".*Federal.*" }, { "id": 22415, "group": 1, "includeFlag": 1, "keyword": ".*Reserve.*" }, { "id": 36326, "group": 3, "includeFlag": 1, "keyword": ".*TALF.*" },{ "id": 33999, "group": 2, "includeFlag": 1, "keyword": ".*tax.*" },{ "id": 36327, "group": 4, "includeFlag": 1, "keyword": ".*Term.*asset.*backed.*securit.*loan.*facilit.*" },{ "id": 33998, "group": 2, "includeFlag": 1, "keyword": ".*treasur.*" }]

# parameters = { "LHSTags": [ { "value": "DBFR" },{ "value": "UDFR" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBFR or UDFR but tag given is other than DBFR or UDFR" } ] }

# associatedStrings=[{ "id": 22413, "group": 1, "includeFlag": 1, "keyword": ".*bond.*" }, { "id": 22412, "group": 1, "includeFlag": 1, "keyword": ".*Mortgage.*" }]

# parameters = { "LHSTags": [ { "value": "DBMB" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBMB but tag given is other than DBMB" } ] }

# associatedStrings=[{ "id": 22388, "group": 4, "includeFlag": 0, "keyword": ".*(note).*" },{ "id": 33995, "group": 5, "includeFlag": 0, "keyword": ".*bank.*" },{ "id": 36323, "group": 10, "includeFlag": 0, "keyword": ".*Discount.*" },{ "id": 36325, "group": 12, "includeFlag": 0, "keyword": ".*Interest.*" },{ "id": 22387, "group": 2, "includeFlag": 0, "keyword": ".*Loan.*" },{ "id": 22386, "group": 1, "includeFlag": 0, "keyword": ".*Mortgage.*" },{ "id": 36324, "group": 11, "includeFlag": 0, "keyword": ".*Premium.*" }, { "id": 33996, "group": 6, "includeFlag": 1, "keyword": ".*EMTN.*" }, { "id": 36321, "group": 8, "includeFlag": 1, "keyword": ".*Loan.*Capital.*" }, { "id": 22392, "group": 1, "includeFlag": 1, "keyword": ".*note.*" },{ "id": 22393, "group": 1, "includeFlag": 1, "keyword": ".*Payab.*" },{ "id": 36322, "group": 9, "includeFlag": 1, "keyword": ".*Promissory.*Note.*" },{ "id": 33997, "group": 7, "includeFlag": 1, "keyword": ".*term.*note.*" }]

# parameters = { "LHSTags": [ { "value": "DBNP" },{ "value": "MCNP" },{ "value": "UDNP" }], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBNP or MCNP or UDNP but tag given is other than DBNP or MCNP or UDNP" } ] }

# associatedStrings=[{ "id": 22172, "group": 2, "includeFlag": 1, "keyword": ".*lend.*" }, { "id": 22171, "group": 2, "includeFlag": 1, "keyword": ".*Securit.*" },{ "id": 36319, "group": 4, "includeFlag": 1, "keyword": ".*Securit.*Borrowed.*" },{ "id": 36320, "group": 5, "includeFlag": 1, "keyword": ".*Securit.*loaned.*" }]

# parameters = { "LHSTags": [ { "value": "DBSL" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBSL but tag given is other than DBSL" } ] }

# associatedStrings=[{ "id": 33994, "group": 4, "includeFlag": 1, "keyword": ".*doll.*roll.*" }, { "id": 33993, "group": 3, "includeFlag": 1, "keyword": ".*REPO.*" },{ "id": 22166, "group": 1, "includeFlag": 1, "keyword": ".*Repurchase.*" }]

# parameters = { "LHSTags": [ { "value": "DBRP" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBRP but tag given is other than DBRP" } ] }

# associatedStrings=[{ "id": 22164, "group": 1, "includeFlag": 1, "keyword": ".*Federa.*" }, { "id": 22165, "group": 1, "includeFlag": 1, "keyword": ".*fund.*" }]

# parameters = { "LHSTags": [ { "value": "DBFF" },{ "value": "UDFF" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBFF or UDFF but tag given is other than DBFF or UDFF" } ] }

# associatedStrings=[{ "id": 22162, "group": 2, "includeFlag": 1, "keyword": ".*Federal.*" }, { "id": 22161, "group": 1, "includeFlag": 1, "keyword": ".*FHLB.*" }, { "id": 22163, "group": 2, "includeFlag": 1, "keyword": ".*Home.*" }]

# parameters = { "LHSTags": [ { "value": "DBFH" },{ "value": "MCFH" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBFH or MCFH but tag given is other than DBFH or MCFH" } ] }

# associatedStrings=[{ "id": 22149, "group": 3, "includeFlag": 1, "keyword": ".*Financ.*" }, { "id": 22150, "group": 3, "includeFlag": 1, "keyword": ".*leas.*" }, { "id": 33988, "group": 4, "includeFlag": 1, "keyword": ".*leasing.*" }, { "id": 33989, "group": 5, "includeFlag": 1, "keyword": ".*leas.*" }, { "id": 33990, "group": 5, "includeFlag": 1, "keyword": ".*oblig.*" }, { "id": 33992, "group": 6, "includeFlag": 1, "keyword": ".*capital.*" }, { "id": 33991, "group": 6, "includeFlag": 1, "keyword": ".*leas.*" }, { "id": 36318, "group": 7, "includeFlag": 1, "keyword": ".*Sale.*and.*lease.*back.*" }]

# parameters = { "LHSTags": [ { "value": "DBCL" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBCL but tag given is other than DBCL" } ] }

# associatedStrings=[{ "id": 22144, "group": 1, "includeFlag": 1, "keyword": ".*Debent.*" }, { "id": 36315, "group": 2, "includeFlag": 0, "keyword": ".*Discount.*" }, { "id": 36317, "group": 4, "includeFlag": 0, "keyword": ".*Interest.*" }, { "id": 36316, "group": 3, "includeFlag": 0, "keyword": ".*Premium.*" }]

# parameters = { "LHSTags": [ { "value": "DBDB" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBDB but tag given is other than DBDB" } ] }

# associatedStrings=[{ "id": 36312, "group": 8, "includeFlag": 0, "keyword": ".*Discount.*" }, { "id": 36314, "group": 10, "includeFlag": 0, "keyword": ".*Interest.*" }, { "id": 33987, "group": 6, "includeFlag": 0, "keyword": ".*money.*market.*" }, { "id": 22139, "group": 1, "includeFlag": 0, "keyword": ".*Mortgage.*" }, { "id": 36313, "group": 9, "includeFlag": 0, "keyword": ".*Premium.*" }, { "id": 22140, "group": 1, "includeFlag": 1, "keyword": ".*Bond.*" }, { "id": 33986, "group": 6, "includeFlag": 1, "keyword": ".*certificat.*" }, { "id": 22143, "group": 4, "includeFlag": 1, "keyword": ".*Hybrid.*" }, { "id": 33985, "group": 5, "includeFlag": 1, "keyword": ".*ICULS.*" }, { "id": 22141, "group": 2, "includeFlag": 1, "keyword": ".*loan stock.*" }, { "id": 36311, "group": 7, "includeFlag": 1, "keyword": ".*Negotiable.*debt.*" }, { "id": 22142, "group": 3, "includeFlag": 1, "keyword": ".*SUKUK.*" }]

# parameters = { "LHSTags": [ { "value": "DBBN" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBBN but tag given is other than DBBN" } ] }

# associatedStrings=[{ "id": 33984, "group": 2, "includeFlag": 0, "keyword": ".*Bank.*" }, { "id": 22137, "group": 1, "includeFlag": 0, "keyword": ".*Loan.*" }, { "id": 22138, "group": 1, "includeFlag": 1, "keyword": ".*Mortgag.*" }]

# parameters = { "LHSTags": [ { "value": "DBMN" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBMN but tag given is other than DBMN" } ] }

# associatedStrings=[{ "id": 33983, "group": 2, "includeFlag": 0, "keyword": ".*Bank.*" }, { "id": 22136, "group": 1, "includeFlag": 1, "keyword": ".*Loan.*" }, { "id": 22135, "group": 1, "includeFlag": 1, "keyword": ".*Mortgage.*" }, { "id": 36309, "group": 3, "includeFlag": 1, "keyword": ".*Mortgage.*credit.*institution.*" }, { "id": 36310, "group": 4, "includeFlag": 1, "keyword": ".*Mortgage.*financial.*institution.*" }]

# parameters = { "LHSTags": [ { "value": "DBML" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBML but tag given is other than DBML" } ] }

# associatedStrings=[{ "id": 36308, "group": 18, "includeFlag": 0, "keyword": ".*Revolving.*" }, { "id": 22116, "group": 1, "includeFlag": 1, "keyword": ".*Bank.*" }, { "id": 22121, "group": 3, "includeFlag": 1, "keyword": ".*bank.*" }, { "id": 22123, "group": 4, "includeFlag": 1, "keyword": ".*bank.*" }, { "id": 22124, "group": 5, "includeFlag": 1, "keyword": ".*bank.*" }, { "id": 22127, "group": 6, "includeFlag": 1, "keyword": ".*bank.*" }, { "id": 22128, "group": 7, "includeFlag": 1, "keyword": ".*Bank.*" }, { "id": 33976, "group": 11, "includeFlag": 1, "keyword": ".*Bank.*" }, { "id": 22130, "group": 8, "includeFlag": 1, "keyword": ".*Bill.*" }, { "id": 22132, "group": 9, "includeFlag": 1, "keyword": ".*BNDES.*" }, { "id": 22125, "group": 5, "includeFlag": 1, "keyword": ".*borrow.*" }, { "id": 22129, "group": 7, "includeFlag": 1, "keyword": ".*debt.*" }, { "id": 22126, "group": 6, "includeFlag": 1, "keyword": ".*Deposit.*" }, { "id": 22131, "group": 8, "includeFlag": 1, "keyword": ".*discount.*" }, { "id": 22120, "group": 3, "includeFlag": 1, "keyword": ".*Due.*" }, { "id": 33980, "group": 14, "includeFlag": 1, "keyword": ".*ICBC.*" }, { "id": 33979, "group": 13, "includeFlag": 1, "keyword": ".*IDBI.*" }, { "id": 36307, "group": 17, "includeFlag": 1, "keyword": ".*KFW.*" }, { "id": 22117, "group": 1, "includeFlag": 1, "keyword": ".*Loan.*" }, { "id": 33981, "group": 15, "includeFlag": 1, "keyword": ".*OBC.*" }, { "id": 33977, "group": 11, "includeFlag": 1, "keyword": ".*obligati.*" }, { "id": 22122, "group": 4, "includeFlag": 1, "keyword": ".*owe.*" }, { "id": 33982, "group": 16, "includeFlag": 1, "keyword": ".*Packing.*Credit.*" }, { "id": 33978, "group": 12, "includeFlag": 1, "keyword": ".*Packing.*SBI.*" }]

# parameters = { "LHSTags": [ { "value": "DBBL" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBBL but tag given is other than DBBL" } ] }

# associatedStrings=[{'id': 33975,'group': 18,'includeFlag': 0,'keyword': '.*Bank.*'},{'id': 33965,'group': 8,'includeFlag': 1,'keyword': '.*bridge.*financ.*'},{'id': 33971,'group':14,'includeFlag': 1,'keyword': '.*construct.*financ.*'},{'id': 33972,'group': 15,'includeFlag': 1,'keyword': '.*credit.*insti.*'},{'id': 33966,'group': 9,'includeFlag': 1,'keyword': '.*equipment.*financ.*'},{'id': 36304,'group': 20,'includeFlag': 1,'keyword': '.*Export.*Finance.*'},{'id': 36303,'group': 19,'includeFlag': 1,'keyword': '.*Factoring.*'},{'id': 33973,'group': 16,'includeFlag': 1,'keyword': '.*financ.*insti.*'},{'id': 33974,'group': 17,'includeFlag': 1,'keyword': '.*government.*'},{'id': 36306,'group': 22,'includeFlag': 1,'keyword': '.*Home.*Equity.*Line.*'},{'id': 33968,'group': 11,'includeFlag': 1,'keyword': '.*import.*financ.*'},{'id': 36305,'group': 21,'includeFlag': 1,'keyword': '.*Insurance.*Compan.*'},{'id': 22108,'group': 1,'includeFlag': 1,'keyword': '.*Loan.*'},{'id': 33964,'group': 7,'includeFlag': 1,'keyword': '.*Non.*revolv.*'},{'id':33970,'group': 13,'includeFlag': 1,'keyword': '.*project.*financ.*'},{'id': 22107,'group': 1,'includeFlag': 1,'keyword': '.*Term.*'},{'id': 33969,'group': 12,'includeFlag': 1,'keyword': '.*vendor.*financ.*'}]

# parameters = { "LHSTags": [ { "value": "DBTL" },{ "value": "MCTL" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBTL or MCTL but tag given is other than DBTL or MCTL" } ] }

# associatedStrings =[{"id": 38478,"group": 14,"includeFlag": 0,"keyword": ".*Trust.*receipt.*"},{"id": 36301,"group": 23,"includeFlag": 1,"keyword": ".*Bank.*Loan.*Agreement.*"},{"id": 36302,"group": 24,"includeFlag": 1,"keyword": ".*Bank.*Loan.*Arrangement.*"},{"id": 22018,"group": 7,"includeFlag": 1,"keyword": ".*capital.*"},{"id": 22024,"group": 10,"includeFlag": 1,"keyword": ".*capital.*"},{"id": 22022,"group": 9,"includeFlag": 1,"keyword": ".*card.*"},{"id": 22019,"group": 8,"includeFlag": 1,"keyword": ".*cash.*"},{"id": 2008,"group": 2,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 22009,"group": 3,"includeFlag": 1,"keyword": ".*Credit.*"},{"id": 22020,"group": 8,"includeFlag": 1,"keyword": ".credit.*"},{"id": 22021,"group": 9,"includeFlag": 1,"keyword": ".*credit.*"},{"id": 36299,"group": 21,"includeFlag": 1,"keyword": ".*Credit.*Agreement.*"},{"id": 36300,"group": 22,"includeFlag": 1,"keyword": ".*Credit.*Arrangement.*"},{"id": 36296,"group": 18,"includeFlag": 1,"keyword": ".*Debtors.*In.*Possession.*Facility"},{"id": 36295,"group": 17,"includeFlag": 1,"keyword": ".*DIP.*Facility.*"},{"id": 22010,"group": 3,"includeFlag": 1,"keyword": ".*facilit.*"},{"id": 22014,"group": 5,"includeFlag": 1,"keyword": ".*facilit.*"},{"id": 33960,"group": 11,"includeFlag": 1,"keyword": ".*facilit.*fund.*"},{"id": 22016,"group": 6,"includeFlag": 1,"keyword": ".*finan.*"},{"id": 36294,"group": 16,"includeFlag": 1,"keyword": ".*Floor.*Plan.*Payable.*"},{"id": 36298,"group": 20,"includeFlag": 1,"keyword": ".*Invoice.*Discounting.*Facility.*"},{"id": 22007,"group": 2,"includeFlag": 1,"keyword": ".*line.*"},{"id": 22023,"group": 10,"includeFlag": 1,"keyword": ".*operating.*"},{"id": 33962,"group": 13,"includeFlag": 1,"keyword": ".*operating.*fund.*"},{"id": 36297,"group": 19,"includeFlag": 1,"keyword": ".*Receivable.*Facility"},{"id": 22006,"group": 1,"includeFlag": 1,"keyword": ".*Revolv.*"},{"id": 22015,"group": 6,"includeFlag": 1,"keyword": ".*running.*"},{"id": 36293,"group": 15,"includeFlag": 1,"keyword": ".*Swing.*Line.*"},{"id": 33963,"group": 14,"includeFlag": 1,"keyword": ".*trust.*receipt.*"},{"id": 22013,"group": 5,"includeFlag": 1,"keyword": ".*warehouse.*"},{"id": 22017,"group": 7,"includeFlag": 1,"keyword": ".*Working.*"},{"id": 33961,"group": 12,"includeFlag": 1,"keyword": ".*working.*fund.*"}]

# parameters = { "LHSTags": [ { "value": "DBRC" },{ "value": "MCRC" },{ "value": "UDRC" } ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBRC or MCRC or UDRC but tag given is other than DBRC or MCRC or UDRC" } ] }

# associatedStrings=[{ "id": 36290, "group": 3, "includeFlag": 0, "keyword": ".*Discount.*" }, { "id": 36292, "group": 5, "includeFlag": 0, "keyword": ".*Interest.*" }, { "id": 36291, "group": 4, "includeFlag": 0, "keyword": ".*Premium.*" }, { "id": 21983, "group": 1, "includeFlag": 1, "keyword": ".*Commer.*" }, { "id": 21986, "group": 2, "includeFlag": 1, "keyword": ".*market.*" }, { "id": 21985, "group": 2, "includeFlag": 1, "keyword": ".*Money.*" }, { "id": 21984, "group": 1, "includeFlag": 1, "keyword": ".*Paper.*" }, { "id": 21987, "group": 2, "includeFlag": 1, "keyword": ".*paper.*" }]

# parameters = { "LHSTags": [ { "value": "DBCP" },{ "value": "MCCP" } ,{ "value": "UDCP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBCP or MCCP or UDCP but tag given is other than DBCP or MCCP or UDCP(Commercial Paper)" } ] }

# associatedStrings=[{ "id": 34019, "group": 5, "includeFlag": 1, "keyword": ".*curren.*accoun.*" }, { "id": 22904, "group": 2, "includeFlag": 1, "keyword": ".*draft.*" }, { "id": 22903, "group": 2, "includeFlag": 1, "keyword": ".*over.*" }, { "id": 22902, "group": 1, "includeFlag": 1, "keyword": ".*overdraft.*" }]

# parameters = { "LHSTags": [ { "value": "DBBO" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBBO but tag given is other than DBBO" } ] }

# associatedStrings=[{"id": 22930,"group": 11,"includeFlag": 1,"keyword": ".*accept.*"},{"id": 34021,"group": 12,"includeFlag": 1,"keyword": ".*Acceptan.*"},{"id": 34023,"group": 13,"includeFlag": 1,"keyword": ".*Acceptan.*"},{"id": 22929,"group": 11,"includeFlag": 1,"keyword": ".*Bank.*"},{"id": 22909,"group": 1,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22911,"group": 2,"includeFlag": 1,"keyword": ".*bill.*"},{"id": 22913,"group": 3,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22914,"group": 4,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22916,"group": 5,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22920,"group": 6,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22922,"group": 7,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22923,"group": 8,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22926,"group": 9,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 34020,"group": 12,"includeFlag": 1,"keyword": ".*Bill.*"},{"id": 22919,"group": 6,"includeFlag": 1,"keyword": ".*Card.*"},{"id": 34022,"group": 13,"includeFlag": 1,"keyword": ".*Commer.*"},{"id": 22908,"group": 1,"includeFlag": 1,"keyword": ".*Commercial.*"},{"id": 22918,"group": 6,"includeFlag": 1,"keyword": ".*Credit.*"},{"id": 22915,"group": 4,"includeFlag": 1,"keyword": ".*Drawn.*"},{"id": 22917,"group": 5,"includeFlag": 1,"keyword": ".*Exchange.*"},{"id": 22925,"group": 9,"includeFlag": 1,"keyword": ".*Export.*"},{"id": 22924,"group": 8,"includeFlag": 1,"keyword": ".*Payable.*"},{"id": 22912,"group": 3,"includeFlag": 1,"keyword": ".*Secured.*"},{"id": 22921,"group": 7,"includeFlag": 1,"keyword": ".*Trade.*"},{"id": 22910,"group": 2,"includeFlag": 1,"keyword": ".*Treasury.*"}]

# parameters = { "LHSTags": [ { "value": "DBBP" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBBO but tag given is other than DBBP" } ] }

# associatedStrings=[{ "id": 36328, "group": 3, "includeFlag": 1, "keyword": ".*Hedging.*and.*Derivative.*Adjustment.*" }]

# parameters = { "LHSTags": [ { "value": "DBHAD" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBHAD but tag given is other than DBHAD" } ] }

# associatedStrings=[{"id": 34028,"group": 3,"includeFlag": 0,"keyword": ".*bond.*"},{"id": 34031,"group": 6,"includeFlag": 0,"keyword": ".*commer.*"},{"id": 34029,"group": 4,"includeFlag": 0,"keyword": ".*note.*"},{"id": 34032,"group": 6,"includeFlag": 0,"keyword": ".*paper.*"},{"id": 34030,"group": 5,"includeFlag": 0,"keyword": ".*revolv.*"},{"id": 34033,"group": 7,"includeFlag": 1,"keyword": ".*Asset.*"},{"id": 34034,"group": 7,"includeFlag": 1,"keyword": ".*Backe.*"},{"id": 28004,"group": 2,"includeFlag": 1,"keyword": ".*securitis.*"},{"id": 28005,"group": 1,"includeFlag": 1,"keyword": ".*securitiz.*"}]

# parameters = { "LHSTags": [ { "value": "DBSF" },{ "value": "MCSF" },{ "value": "UDSF" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBSF or MCSF or UDSF but tag given is other than DBSF or MCSF or UDSF" } ] }

# associatedStrings=[{"id": 32219,"group": 4,"includeFlag": 0,"keyword": ".*Bank.*"},{"id": 32220,"group": 10,"includeFlag": 0,"keyword": ".*Bond.*"},{"id": 32222,"group": 15,"includeFlag": 0,"keyword": ".*Certificate.*"},{"id": 32223,"group": 11,"includeFlag": 0,"keyword": ".*Commercial Paper.*"},{"id": 32224,"group": 14,"includeFlag": 0,"keyword": ".*Credit Agreement.*"},{"id": 32225,"group": 1,"includeFlag": 0,"keyword": ".*Credit Institution.*"},{"id": 34024,"group": 18,"includeFlag": 0,"keyword": ".*Debent.*"},{"id": 32227,"group": 3,"includeFlag": 0,"keyword": ".*Facilit.*"},{"id": 32228,"group": 2,"includeFlag": 0,"keyword": ".*Financial Institution.*"},{"id": 32230,"group": 16,"includeFlag": 0,"keyword": ".*Lease.*"},{"id": 32231,"group": 8,"includeFlag": 0,"keyword": ".*Letter.*"},{"id": 32232,"group": 13,"includeFlag": 0,"keyword": ".*Line.*"},{"id": 32233,"group": 7,"includeFlag": 0,"keyword": ".*Loan.*"},{"id": 32235,"group": 9,"includeFlag": 0,"keyword": ".*Mortgage.*"},{"id": 32236,"group": 6,"includeFlag": 0,"keyword": ".*Note.*"},{"id": 32237,"group": 12,"includeFlag": 0,"keyword": ".*Paper.*"},{"id": 32239,"group": 17,"includeFlag": 0,"keyword": ".*prefer.*debt.*securit.*"},{"id": 32242,"group": 5,"includeFlag": 0,"keyword": ".*Revolv.*"},{"id": 32221,"group": 8,"includeFlag": 1,"keyword": ".*Call Money.*"},{"id": 32226,"group": 3,"includeFlag": 1,"keyword": ".*Debt Securities.*"},{"id": 32229,"group": 9,"includeFlag": 1,"keyword": ".*Hire purchase.*"},{"id": 34025,"group": 19,"includeFlag": 1,"keyword": ".*Mandator.*"},{"id": 32234,"group": 7,"includeFlag": 1,"keyword": ".*Money Market.*"},{"id": 34027,"group": 19,"includeFlag": 1,"keyword": ".*Prefer.*"},{"id": 32240,"group": 10,"includeFlag": 1,"keyword": ".*Private placement.*"},{"id": 34026,"group": 19,"includeFlag": 1,"keyword": ".*Rede.*"},{"id": 32243,"group": 2,"includeFlag": 1,"keyword": ".*Sales Tax.*"},{"id": 32244,"group": 5,"includeFlag": 1,"keyword": ".*term credit.*"},{"id": 32245,"group": 4,"includeFlag": 1,"keyword": ".*term debt.*"},{"id": 32246,"group": 6,"includeFlag": 1,"keyword": ".*term finance.*"}]

# parameters = { "LHSTags": [ { "value": "DBOB" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBOB but tag given is other than DBOB (Other Borrowings)" } ] }

associatedStrings=[{ "id": 34387, "group": 1, "includeFlag": 1, "keyword": ".*Borrow.*" },{ "id": 34386, "group": 1, "includeFlag": 1, "keyword": ".*Gener.*" }]

parameters = { "LHSTags": [ { "value": "DBGB" }  ], "BlockId": [ { "value": "568" } ], "Main/Breakup/All": [ { "value": "All" } ], "StatementType": [ { "value": "DCS" } ], "CheckDescription": [ { "value": "Line item description having the strings related to tag DBGB but tag given is other than DBGB (General/Other Borrowings)" } ] }


def T13(historicalData,filingMetadata,extractedData,parameters):

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
                param_id.append(pam.get('id'))
        except:
            param_id=''
        return param_id 


    industry_inc=get_param_id(parameters,'INDUSTRY_INCLUDE')
    industry_exc=get_param_id(parameters,'INDUSTRY_EXCLUDE')
    block_id1=get_param_value(parameters,'BlockId')
    template=get_param_id(parameters,'Template')
    gaap_inc=get_param_value(parameters,'GAAP_INCLUDE')
    gaap_exc=get_param_value(parameters,'GAAP_EXCLUDE')
    country_inc=get_param_value(parameters,'COUNTRY_INCLUDE')
    country_exc=get_param_value(parameters,'COUNTRY_EXCLUDE')
    AsReportedPeriodEndDate=get_param_value(parameters,'AsReportedPeriodEndDate')
    PeriodTypeName=get_param_value(parameters,'PeriodTypeName')
    Preliminaryflag=get_param_value(parameters,'preliminaryFlag')
    SNGBCflag=get_param_value(parameters,'SNGBC/PPR/DMCF flag')
    #lhs_tag=parameters['LHSTags'][0]['value'].split(',')
    lhs_tag=get_param_value(parameters,'LHSTags')
    lhs_tags=[]
    for tg in lhs_tag:
        lhs_tags.append(str(tg))
        lhs_tags.append(str(tg)+'#CV')
        lhs_tags.append(str(tg)+'#IL')
    
    exclude_tag=get_param_value(parameters,'ExcludeDataitemTag')
    exclude_tags=[]
    for tg in exclude_tag:
        exclude_tags.append(str(tg))
        exclude_tags.append(str(tg)+'#CV')
        exclude_tags.append(str(tg)+'#IL')
    #print(exclude_tags)    
    MBSflag=get_param_value(parameters,'Main/Breakup/All')
    if MBSflag=="":
        MBSflag='All'
    stmt=parameters['StatementType'][0]['value']
    #print(stmt)
    block_id=[]
    if len(block_id1)>0:
        for ele in block_id1:
            block_id.append(ele.strip())



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
    high=[]
    CTC=[]
    tag=get_param_value(parameters,'LHSTags')
    filingid=filingMetadata['metadata']['filingId']
    #validating parameter values
    #print(tag)

    if (len(template)==0 or filingMetadata["metadata"]["templateId"] in template)\
    and (len(industry_inc)==0 or filingMetadata["metadata"]["industryId"] in industry_inc)\
    and (len(industry_exc)==0 or filingMetadata["metadata"]["industryId"] not in industry_exc)\
    and (len(country_inc)==0 or filingMetadata["metadata"]["countryCode"] in country_inc)\
    and (len(country_exc)==0 or filingMetadata["metadata"]["countryCode"] not in country_exc)\
    and (len(PeriodTypeName)==0 or filingMetadata["metadata"]["periodType"] in PeriodTypeName)\
    and (len(Preliminaryflag)==0 or filingMetadata["metadata"]["preliminaryFlag"] in Preliminaryflag)\
    and (AsReportedPeriodEndDate=="" or check_period(ARoperator))\
    and (len(SNGBCflag)==0 or not(any(item in SNGBCflag for item in list(filingMetadata['metadataTags'].keys()))))\
    and len(tag)!=0:
        
    #and (gaap_inc=="" or filingMetadata["metadata"]["GAAP"] in gaap_inc)\
    #and (gaap_exc=="" or filingMetadata["metadata"]["GAAP"] not in gaap_exc)\	

        import re

                
        high1=[]
        high=[]

        includeFlag_counts = {}

# Iterate over the associatedStrings
        for item in associatedStrings:
            group = item["group"]
            includeFlag = item["includeFlag"]

            # Update the count of includeFlag for the group
            if group in includeFlag_counts:
                includeFlag_counts[group][includeFlag] = includeFlag_counts[group].get(includeFlag, 0) + 1
            else:
                includeFlag_counts[group] = {0: 0, 1: 0}
                includeFlag_counts[group][includeFlag] = 1

        for key in extractedData.keys():
            for ins in extractedData[key].keys():
                if extractedData[key][ins]['section']==stmt:
                    if 'childRows' in extractedData[key][ins].keys() and ('Breakups Only' in MBSflag or 'All' in MBSflag):
                        for chins in extractedData[key][ins]['childRows'].keys():
                            if extractedData[key][ins]['childRows'][chins]['tag'] not in lhs_tags and extractedData[key][ins]['childRows'][chins]['tag'] not in exclude_tags and str(extractedData[key][ins]['childRows'][chins]['blockId']) in block_id:
                                high1.append({'tag':extractedData[key][ins]['childRows'][chins]['tag'],'description':extractedData[key][ins]['childRows'][chins]['description'],'ins':chins})

                    if 'childRows' not in extractedData[key][ins].keys() and ('Main Only' in MBSflag or 'All' in MBSflag) and extractedData[key][ins]['isChildRow'] is False:
                        if extractedData[key][ins]['tag'] not in lhs_tags and extractedData[key][ins]['tag'] not in exclude_tags and str(extractedData[key][ins]['blockId']) in block_id:
                            high1.append({'tag':extractedData[key][ins]['tag'],'description':extractedData[key][ins]['description'],'ins':ins})

        #print(high1)
        current_results = {}
        for reported_string in high1:
            # Iterate over associated strings
            for assoc_str in associatedStrings:
                group = assoc_str["group"]
                include_flag = assoc_str["includeFlag"]
                tag = reported_string['tag']
                ins = reported_string['ins']

                # Create tag entry if it doesn't exist
                if tag not in current_results:
                    current_results[tag] = {}

                # Create group entry if it doesn't exist
                if group not in current_results[tag]:
                    current_results[tag][group] = {}

                # Create ins entry if it doesn't exist
                if ins not in current_results[tag][group]:
                    current_results[tag][group][ins] = {0: 0, 1: 0, "highlights": []}

                # Check if any word matches the associated string pattern
                if re.search(assoc_str["keyword"], reported_string['description'], re.IGNORECASE):
                    current_results[tag][group][ins][include_flag] += 1
                    current_results[tag][group][ins]["highlights"].append(reported_string)

        #print(associatedStrings,current_results,high1)


        filtered_results = {tag: {group: {ins: values for ins, values in ins_dict.items() if len(values["highlights"]) > 0}
                                for group, ins_dict in group_dict.items() if any(len(values["highlights"]) > 0 for ins, values in ins_dict.items())}
                            for tag, group_dict in current_results.items() if any(len(values["highlights"]) > 0 for group, ins_dict in group_dict.items() for ins, values in ins_dict.items())}

        #print(filtered_results)
        matched_results = {}
        matched_groups = {}

        for tag, group_data in filtered_results.items():
            for group, instances in group_data.items():
                for instance, innerloop in instances.items():
                    include_flag_count = includeFlag_counts.get(group, {}).get(1, 0)
                    if innerloop[1] == include_flag_count and innerloop[0] == 0:
                        matched_groups[group] = innerloop
            if matched_groups:
                matched_results[tag] = matched_groups

        high = []
        unique_entries = set()

        for tag, groups in matched_results.items():
            for group, values in groups.items():
                highvalues = values.get('highlights', [])
                for item in highvalues:
                    item_tag = item['tag']
                    item_ins = item['ins']
                    item_description = item['description']
                    entry = (item_tag, item_ins, item_description)
                    if entry not in unique_entries:
                        unique_entries.add(entry)
                        high.append({ "section": stmt, "row": { "name": item_tag,"id": item_ins},"filingId": int(filingMetadata['metadata']['filingId'])})
                        CTC.append({"statement": stmt, "tag": item_tag, "description": item_description,"filingId": filingid})
        #print(CTC)
    if len(high)>0:
        err="Line item description having the strings related to tag" + str(lhs_tag) +" but given is other than the tag" 
        result={"highlights": [],"error":""}
        result["highlights"]=high     
        result["error"] = err
        result["checkGeneratedFor"]={"statement": stmt, "tag": str(lhs_tag), "description": "", "refFilingId": "","filingId": int(filingid),"objectId": "", "peo": "","fpo": "", "diff": ""}
        result["checkGeneratedForList"]=CTC
        results.append(result)

    return results    
   
if __name__ == '__main__':
  start_time = time.time()
  results1 = T13(historicalData,filingMetadata,extractedData,parameters)
  print(results1)
  print('Total time: ', time.time() - start_time)
    
