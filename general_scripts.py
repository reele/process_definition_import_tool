
etl_jobs = [
    # ['group','theme','etl_job','description','cycle','day_offset','osprogram','workdir','scriptname','scriptpath','additionparameters','enable'],
    ['FLG','S01','HEAD','S01头部作业','D','0','perl','','s01_head.pl','/DW/etl/app/flg/S01/bin/','','1'],
    ['ODB','S01','SOURCE_TABLE','源表贴源','D','0','perl','','s01_source_table.pl','/DW/etl/app/odb/S01/S01_SOURCE_TABLE/bin/','','1'],
    ['CVT','S01','SOURCE_TABLE','源表清洗','D','0','perl','','s01_source_table.pl','/DW/etl/app/cvt/S01/S01_SOURCE_TABLE/bin/','','1'],
    ['SDB','S01','SOURCE_TABLE','源表缓冲','D','0','perl','','s01_source_table.pl','/DW/etl/app/sdb/S01/S01_SOURCE_TABLE/bin/','','1'],

    ['PDB','T01','CUST','客户','D','0','perl','','t01_cust.pl','/DW/etl/app/pdb/T01/T01_CUST/bin/','','1'],
    ['PDB','T02','ACCT','账户','D','0','perl','','t02_acct.pl','/DW/etl/app/pdb/T02/T02_ACCT/bin/','','1'],
    ['CDB','T88','STAT','xxxxxx','D','0','perl','','t88_stat.pl','/DW/etl/app/cdb/T88/T88_STAT/bin/','','1'],
    ['DDB','DDB','STAT','export','D','0','perl','','ddb_stat.pl','/DW/etl/app/ddb/theme/DDB_STAT/bin/','','1'],
]

etl_dependencies = [
    # ['group','theme','etl_job','dependency_group','dependency_theme','dependency_etl_job'],
    ['CVT','S01','SOURCE_TABLE','FLG','S01','HEAD'],
    ['SDB','S01','SOURCE_TABLE','CVT','S01','SOURCE_TABLE'],
    ['ODB','S01','SOURCE_TABLE','SDB','S01','SOURCE_TABLE'],

    ['PDB','T01','CUST','ODB','S01','SOURCE_TABLE'],
    ['PDB','T02','ACCT','ODB','S01','SOURCE_TABLE'],

    ['CDB','T88','STAT','PDB','T01','CUST'],
    ['CDB','T88','STAT','PDB','T02','ACCT'],

    ['DDB','DDB','STAT','CDB','T88','STAT'],
]