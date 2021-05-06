/* LOAD INFLOW INVENTORY DATA FOR OFFICE INVENTORY */

/* ADAPTED FOR RUNNING ON SERVER / GOOGLE CLOUD */



/* PRODUCT DB */

/* INFLOW PRODUCT DETAILS DUMPED UP FROM IPA PROCESSING */

-- select * from carawdata.soss_inflow_productdetails_raw ;

alter table carawdata.soss_inflow_productdetails_raw convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;
-- create index inFlow_ii1 on carawdata.soss_inflow_productdetails_raw(name_inFlow);
/* STOCK LEVELS */
/* FROM INFLOW API SCRIPT */

-- select * from carawdata.inflow_live_inventory ;

/* CREATE WORKING TABLE */
drop table if exists carawdata.inflow_live_inventory_trim ;
create table carawdata.inflow_live_inventory_trim
	(item_inflow varchar(150),location_inflow varchar(25),quantity_inflow float)
	;
insert ignore carawdata.inflow_live_inventory_trim
	select 
		ifnull(traw.name,'') as item_inflow,
        ifnull(traw.location_id,'') as location_inflow,
        ifnull(quantity_available,0) as quantity_inflow
	from carawdata.inflow_live_inventory as traw
    where is_active>0
    order by item_inflow
    ;
-- 	select * from carawdata.inflow_live_inventory_trim ;
alter table carawdata.soss_inflow_productdetails_raw convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;


-- drop table if exists carawdata.inflow_live_inventory_trim ;
-- create table carawdata.inflow_live_inventory_trim
--     (item_inflow varchar(150),
-- 	location_inflow varchar(25),
-- 	sublocation_inflow varchar(25),
-- 	serial_inflow varchar(25),
-- 	quantity_inflow int)
--     ;
-- load data local infile '%USER_drivepath%/Shock Surplus Collaborative/Full System Automation/IPA Python/Automation Input Staging/inFlow_StockLevels.csv'
-- 	into table carawdata.inflow_live_inventory_trim
--     character set utf8
-- 	fields terminated by ','
--     optionally enclosed by '"'
-- 	lines terminated by '\r\n'
--     ignore 1 lines
-- 	;
-- create index inFlow_ii2 on carawdata.inflow_live_inventory_trim(item_inflow);
/* IMPORT ASSEMBLED 6112 COST MAPPING */
-- drop table if exists carawdata.assembled_6112_cost_mapping ;
-- create table carawdata.assembled_6112_cost_mapping
-- 	(pn_base_6112 varchar(50),assembled_6112_cost float,
--     mount_1_needed varchar(50),mount_1_qty_needed float,
--     mount_2_needed varchar(50),mount_2_qty_needed float,
--     mount_3_needed varchar(50),mount_3_qty_needed float,
--     backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,
--     backup_mount_2_needed varchar(50),backup_mount_2_qty_needed float
--     )
--     ;
-- load data local infile '%USER_drivepath%/Shock Surplus Collaborative/Pricing/assembled_6112_cost_mapping.csv'
-- 	into table carawdata.assembled_6112_cost_mapping
--     character set utf8mb4
-- 	fields terminated by ','
--     optionally enclosed by '"'
-- 	lines terminated by '\r\n'
--     ignore 1 lines
-- 	;
/* MERGE CURRENT STOCK LEVELS WITH PRICE FROM PRODUCT DETAILS DB; PRODUCT COMMON PART NUMBER */
drop table if exists carawdata.soss_inFlow_StockLevels_00 ;
create table carawdata.soss_inFlow_StockLevels_00
	(name_inflow varchar(75),
    item_inflow varchar(75),
    quantity_inflow float,
    cost_inflow varchar(50),
	part_number_common varchar(50))
	;
insert ignore carawdata.soss_inFlow_StockLevels_00
    select
		name_inflow,
        item_inflow,
        ifnull(quantity_inflow,0) as quantity_inflow,
        cost_inflow,
		replace(replace(name_inflow,'-',''),' ','') as part_number_common
    from carawdata.soss_inflow_productdetails_raw
    left join carawdata.inflow_live_inventory_trim
    on name_inflow=item_inflow
    /* EXCLUDE OLD CC CLIP PARTS */
    where not (ucase(left(name_inflow,3))='BIL' and locate('cc',lcase(name_inflow))>0)
    order by name_inflow
	;
alter table carawdata.soss_inFlow_StockLevels_00 convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;

/* CHECK */
-- 	select * from carawdata.soss_inFlow_StockLevels_00  ;

-- 	select * from carawdata.soss_product_set_lookup ;
    
    
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* TAKE SUBSET OF 6112 ASSEMBLED LISTINGS FROM PRODUCT SET */
drop table if exists carawdata.pset_assem_only ;
create table carawdata.pset_assem_only like carawdata.soss_product_set_lookup ;
insert ignore carawdata.pset_assem_only
	select *
    from carawdata.soss_product_set_lookup
    where locate('assem',lcase(product_line))>0 and locate('6112',lcase(product_line))>0
	;
/* STACK ALL OF THE CC PART NUMBERS TO GET A UNIQUE LIST */
/* SIMULTANEOUSLY MAP TO BASE PART NUMBER */
drop table if exists carawdata.unique_6112_partnumbers ;
create table carawdata.unique_6112_partnumbers
	(pn varchar(75),pn_base_mapping varchar(75)) 
    ;
insert ignore carawdata.unique_6112_partnumbers
	select 
		tt.*,
		left(pn,locate('_',pn)-1) as pn_base_mapping
    from (
		(select pn1p as pn from carawdata.pset_assem_only where pn1p!='' and (locate('46-',pn1p)>0 or locate('47-',pn1p)>0)) union all
		(select pn2p as pn from carawdata.pset_assem_only where pn2p!='' and (locate('46-',pn2p)>0 or locate('47-',pn2p)>0)) union all
		(select pn3p as pn from carawdata.pset_assem_only where pn3p!='' and (locate('46-',pn3p)>0 or locate('47-',pn3p)>0)) union all
		(select pn4p as pn from carawdata.pset_assem_only where pn4p!='' and (locate('46-',pn4p)>0 or locate('47-',pn4p)>0)) union all
		(select pn5p as pn from carawdata.pset_assem_only where pn5p!='' and (locate('46-',pn5p)>0 or locate('47-',pn5p)>0))
		) as tt
	group by pn
    ;
/* JOIN COST ONTO DB BY BASE PART, TABLE PROVIDED AS SEPARATE INPUT */
/* ALSO JOIN ON QUANTITY BY BASE PART FROM CURRENT INFLOW DATA */
/* ALSO JOIN MOUNT 1 AND MOUNT 2 QUANTITY ON BY THE RESPECTIVE MOUNT-NEEDED VARS */
drop table if exists carawdata.unique_6112_with_cost ;
create table carawdata.unique_6112_with_cost
	(pn varchar(50),pn_base_mapping varchar(50),assembled_6112_cost float,quantity_inflow float,pn_assem_common varchar(100),
	mount_1_needed varchar(50),mount_1_qty_needed float,mount_2_needed varchar(50),mount_2_qty_needed float,
	mount_3_needed varchar(50),mount_3_qty_needed float,backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,
	backup_mount_2_needed varchar(50),backup_mount_2_qty_needed float,mount_1_inflow_qty float,mount_2_inflow_qty float,
	mount_3_inflow_qty float,backup_mount_1_inflow_qty float,backup_mount_2_inflow_qty float)
	;
insert ignore carawdata.unique_6112_with_cost
	select 
		pn.*,
        ifnull(assembled_6112_cost,0) as assembled_6112_cost,
        ifnull(stock.quantity_inflow,0) as quantity_inflow,
        replace(replace(pn,'-',''),' ','') as pn_assem_common,
		mount_1_needed,mount_1_qty_needed,
		mount_2_needed,mount_2_qty_needed,
		mount_3_needed,mount_3_qty_needed,
		backup_mount_1_needed,backup_mount_1_qty_needed,
		backup_mount_2_needed,backup_mount_2_qty_needed,
        ifnull(mstock.quantity_inflow,0) as mount_1_inflow_qty,
        ifnull(mstock2.quantity_inflow,0) as mount_2_inflow_qty,
        ifnull(mstock3.quantity_inflow,0) as mount_3_inflow_qty,
        ifnull(b_mstock.quantity_inflow,0) as backup_mount_1_inflow_qty,
        ifnull(b_mstock2.quantity_inflow,0) as backup_mount_2_inflow_qty        
	from carawdata.unique_6112_partnumbers as pn
    left join carawdata.assembled_6112_cost_mapping as cost on pn_base_mapping=pn_base_6112
    left join carawdata.soss_inFlow_StockLevels_00 as stock on pn_base_mapping=stock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock on mount_1_needed=mstock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock2 on mount_2_needed=mstock2.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock3 on mount_3_needed=mstock3.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as b_mstock on backup_mount_1_needed=b_mstock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as b_mstock2 on backup_mount_2_needed=b_mstock2.name_inflow
	;
/* ADJUST ASSEMBLED STOCK FOR AVAILABLE MOUNTS */
drop table if exists carawdata.assem_6112_stock_adjust_for_mounts ;
create table carawdata.assem_6112_stock_adjust_for_mounts
	(pn varchar(50),pn_base_mapping varchar(50),assembled_6112_cost float,quantity_inflow float,pn_assem_common varchar(100),
	mount_1_needed varchar(50),mount_1_qty_needed float,mount_2_needed varchar(50),mount_2_qty_needed float,mount_3_needed varchar(50),
	mount_3_qty_needed float,backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,backup_mount_2_needed varchar(50),
	backup_mount_2_qty_needed float,mount_1_inflow_qty float,mount_2_inflow_qty float,mount_3_inflow_qty float,backup_mount_1_inflow_qty float,
	backup_mount_2_inflow_qty float,assem_6112_adjust_for_mounts double)
	;
insert ignore carawdata.assem_6112_stock_adjust_for_mounts
	select
		cc.*,
        if(
			/* TEMPORARY EXCLUSION FROM ASSEMBLED PROGRAM WHEN NEEDED */
-- 			locate('47-244641',pn)>0 or locate('47-251922',pn)>0 or locate('47-273702',pn)>0,0,            
			locate('999999999',pn)>0,0,            
			least(
				quantity_inflow,
				if(mount_1_qty_needed>0,floor(mount_1_inflow_qty/mount_1_qty_needed),999)+if(mount_1_needed=backup_mount_1_needed,0,if(backup_mount_1_qty_needed>0,floor(backup_mount_1_inflow_qty/backup_mount_1_qty_needed),0)),
				if(mount_2_qty_needed>0,floor(mount_2_inflow_qty/mount_2_qty_needed),999)+if(mount_2_needed=backup_mount_2_needed,0,if(backup_mount_2_qty_needed>0,floor(backup_mount_2_inflow_qty/backup_mount_2_qty_needed),0)),
				if(mount_3_qty_needed>0,floor(mount_3_inflow_qty/mount_3_qty_needed),999)
				)) as assem_6112_adjust_for_mounts
	from carawdata.unique_6112_with_cost as cc
	;
/* STACK INFLOW DATA WITH CIRCLIP 6112 PART NUMBERS THAT NOW INCLUDE STOCK AND COST */
drop table if exists carawdata.inFlow_stack_with_assem_data ;
create table carawdata.inFlow_stack_with_assem_data
	(name_inflow varchar(150),quantity_inflow double,cost_inflow float,part_number_common varchar(100))
    ;
insert ignore carawdata.inFlow_stack_with_assem_data
    (select name_inflow,quantity_inflow,cost_inflow,part_number_common from carawdata.soss_inFlow_StockLevels_00) union all
    (select pn,assem_6112_adjust_for_mounts,assembled_6112_cost,pn_assem_common from carawdata.assem_6112_stock_adjust_for_mounts)
	;
--     select * from carawdata.inFlow_stack_with_assem_data ;
    
    /* LOAD INFLOW INVENTORY DATA FOR OFFICE INVENTORY */

/* ADAPTED FOR RUNNING ON SERVER / GOOGLE CLOUD */



/* PRODUCT DB */

/* INFLOW PRODUCT DETAILS DUMPED UP FROM IPA PROCESSING */

-- select * from carawdata.soss_inflow_productdetails_raw ;

alter table carawdata.soss_inflow_productdetails_raw convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;
-- create index inFlow_ii1 on carawdata.soss_inflow_productdetails_raw(name_inFlow);
/* STOCK LEVELS */
/* FROM INFLOW API SCRIPT */

-- select * from carawdata.inflow_live_inventory ;

/* CREATE WORKING TABLE */
drop table if exists carawdata.inflow_live_inventory_trim ;
create table carawdata.inflow_live_inventory_trim
	(item_inflow varchar(150),location_inflow varchar(25),quantity_inflow float)
	;
insert ignore carawdata.inflow_live_inventory_trim
	select 
		ifnull(traw.name,'') as item_inflow,
        ifnull(traw.location_id,'') as location_inflow,
        ifnull(quantity_available,0) as quantity_inflow
	from carawdata.inflow_live_inventory as traw
    where is_active>0
    order by item_inflow
    ;
-- 	select * from carawdata.inflow_live_inventory_trim ;
alter table carawdata.soss_inflow_productdetails_raw convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;


-- drop table if exists carawdata.inflow_live_inventory_trim ;
-- create table carawdata.inflow_live_inventory_trim
--     (item_inflow varchar(150),
-- 	location_inflow varchar(25),
-- 	sublocation_inflow varchar(25),
-- 	serial_inflow varchar(25),
-- 	quantity_inflow int)
--     ;
-- load data local infile '%USER_drivepath%/Shock Surplus Collaborative/Full System Automation/IPA Python/Automation Input Staging/inFlow_StockLevels.csv'
-- 	into table carawdata.inflow_live_inventory_trim
--     character set utf8
-- 	fields terminated by ','
--     optionally enclosed by '"'
-- 	lines terminated by '\r\n'
--     ignore 1 lines
-- 	;
-- create index inFlow_ii2 on carawdata.inflow_live_inventory_trim(item_inflow);
/* IMPORT ASSEMBLED 6112 COST MAPPING */
-- drop table if exists carawdata.assembled_6112_cost_mapping ;
-- create table carawdata.assembled_6112_cost_mapping
-- 	(pn_base_6112 varchar(50),assembled_6112_cost float,
--     mount_1_needed varchar(50),mount_1_qty_needed float,
--     mount_2_needed varchar(50),mount_2_qty_needed float,
--     mount_3_needed varchar(50),mount_3_qty_needed float,
--     backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,
--     backup_mount_2_needed varchar(50),backup_mount_2_qty_needed float
--     )
--     ;
-- load data local infile '%USER_drivepath%/Shock Surplus Collaborative/Pricing/assembled_6112_cost_mapping.csv'
-- 	into table carawdata.assembled_6112_cost_mapping
--     character set utf8mb4
-- 	fields terminated by ','
--     optionally enclosed by '"'
-- 	lines terminated by '\r\n'
--     ignore 1 lines
-- 	;
/* MERGE CURRENT STOCK LEVELS WITH PRICE FROM PRODUCT DETAILS DB; PRODUCT COMMON PART NUMBER */
drop table if exists carawdata.soss_inFlow_StockLevels_00 ;
create table carawdata.soss_inFlow_StockLevels_00
	(name_inflow varchar(75),
    item_inflow varchar(75),
    quantity_inflow float,
    cost_inflow varchar(50),
	part_number_common varchar(50))
	;
insert ignore carawdata.soss_inFlow_StockLevels_00
    select
		name_inflow,
        item_inflow,
        ifnull(quantity_inflow,0) as quantity_inflow,
        cost_inflow,
		replace(replace(name_inflow,'-',''),' ','') as part_number_common
    from carawdata.soss_inflow_productdetails_raw
    left join carawdata.inflow_live_inventory_trim
    on name_inflow=item_inflow
    /* EXCLUDE OLD CC CLIP PARTS */
    where not (ucase(left(name_inflow,3))='BIL' and locate('cc',lcase(name_inflow))>0)
    order by name_inflow
	;
alter table carawdata.soss_inFlow_StockLevels_00 convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;

/* CHECK */
-- 	select * from carawdata.soss_inFlow_StockLevels_00  ;

    
    
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* START 6112 ASSEMBLED AUGMENTATION OF INFLOW DATA */
/* TAKE SUBSET OF 6112 ASSEMBLED LISTINGS FROM PRODUCT SET */
drop table if exists carawdata.pset_assem_only ;
create table carawdata.pset_assem_only like carawdata.soss_product_set_lookup ;
insert ignore carawdata.pset_assem_only
	select *
    from carawdata.soss_product_set_lookup
    where locate('assem',lcase(product_line))>0 and locate('6112',lcase(product_line))>0
	;
/* STACK ALL OF THE CC PART NUMBERS TO GET A UNIQUE LIST */
/* SIMULTANEOUSLY MAP TO BASE PART NUMBER */
drop table if exists carawdata.unique_6112_partnumbers ;
create table carawdata.unique_6112_partnumbers
	(pn varchar(75),pn_base_mapping varchar(75)) 
    ;
insert ignore carawdata.unique_6112_partnumbers
	select 
		tt.*,
		left(pn,locate('_',pn)-1) as pn_base_mapping
    from (
		(select pn1p as pn from carawdata.pset_assem_only where pn1p!='' and (locate('46-',pn1p)>0 or locate('47-',pn1p)>0)) union all
		(select pn2p as pn from carawdata.pset_assem_only where pn2p!='' and (locate('46-',pn2p)>0 or locate('47-',pn2p)>0)) union all
		(select pn3p as pn from carawdata.pset_assem_only where pn3p!='' and (locate('46-',pn3p)>0 or locate('47-',pn3p)>0)) union all
		(select pn4p as pn from carawdata.pset_assem_only where pn4p!='' and (locate('46-',pn4p)>0 or locate('47-',pn4p)>0)) union all
		(select pn5p as pn from carawdata.pset_assem_only where pn5p!='' and (locate('46-',pn5p)>0 or locate('47-',pn5p)>0))
		) as tt
	group by pn
    ;
/* JOIN COST ONTO DB BY BASE PART, TABLE PROVIDED AS SEPARATE INPUT */
/* ALSO JOIN ON QUANTITY BY BASE PART FROM CURRENT INFLOW DATA */
/* ALSO JOIN MOUNT 1 AND MOUNT 2 QUANTITY ON BY THE RESPECTIVE MOUNT-NEEDED VARS */
drop table if exists carawdata.unique_6112_with_cost ;
create table carawdata.unique_6112_with_cost
	(pn varchar(50),pn_base_mapping varchar(50),assembled_6112_cost float,quantity_inflow float,pn_assem_common varchar(100),
	mount_1_needed varchar(50),mount_1_qty_needed float,mount_2_needed varchar(50),mount_2_qty_needed float,
	mount_3_needed varchar(50),mount_3_qty_needed float,backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,
	backup_mount_2_needed varchar(50),backup_mount_2_qty_needed float,mount_1_inflow_qty float,mount_2_inflow_qty float,
	mount_3_inflow_qty float,backup_mount_1_inflow_qty float,backup_mount_2_inflow_qty float)
	;
insert ignore carawdata.unique_6112_with_cost
	select 
		pn.*,
        ifnull(assembled_6112_cost,0) as assembled_6112_cost,
        ifnull(stock.quantity_inflow,0) as quantity_inflow,
        replace(replace(pn,'-',''),' ','') as pn_assem_common,
		mount_1_needed,mount_1_qty_needed,
		mount_2_needed,mount_2_qty_needed,
		mount_3_needed,mount_3_qty_needed,
		backup_mount_1_needed,backup_mount_1_qty_needed,
		backup_mount_2_needed,backup_mount_2_qty_needed,
        ifnull(mstock.quantity_inflow,0) as mount_1_inflow_qty,
        ifnull(mstock2.quantity_inflow,0) as mount_2_inflow_qty,
        ifnull(mstock3.quantity_inflow,0) as mount_3_inflow_qty,
        ifnull(b_mstock.quantity_inflow,0) as backup_mount_1_inflow_qty,
        ifnull(b_mstock2.quantity_inflow,0) as backup_mount_2_inflow_qty        
	from carawdata.unique_6112_partnumbers as pn
    left join carawdata.assembled_6112_cost_mapping as cost on pn_base_mapping=pn_base_6112
    left join carawdata.soss_inFlow_StockLevels_00 as stock on pn_base_mapping=stock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock on mount_1_needed=mstock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock2 on mount_2_needed=mstock2.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as mstock3 on mount_3_needed=mstock3.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as b_mstock on backup_mount_1_needed=b_mstock.name_inflow
    left join carawdata.soss_inFlow_StockLevels_00 as b_mstock2 on backup_mount_2_needed=b_mstock2.name_inflow
	;
/* ADJUST ASSEMBLED STOCK FOR AVAILABLE MOUNTS */
drop table if exists carawdata.assem_6112_stock_adjust_for_mounts ;
create table carawdata.assem_6112_stock_adjust_for_mounts
	(pn varchar(50),pn_base_mapping varchar(50),assembled_6112_cost float,quantity_inflow float,pn_assem_common varchar(100),
	mount_1_needed varchar(50),mount_1_qty_needed float,mount_2_needed varchar(50),mount_2_qty_needed float,mount_3_needed varchar(50),
	mount_3_qty_needed float,backup_mount_1_needed varchar(50),backup_mount_1_qty_needed float,backup_mount_2_needed varchar(50),
	backup_mount_2_qty_needed float,mount_1_inflow_qty float,mount_2_inflow_qty float,mount_3_inflow_qty float,backup_mount_1_inflow_qty float,
	backup_mount_2_inflow_qty float,assem_6112_adjust_for_mounts double)
	;
insert ignore carawdata.assem_6112_stock_adjust_for_mounts
	select
		cc.*,
        if(
			/* TEMPORARY EXCLUSION FROM ASSEMBLED PROGRAM WHEN NEEDED */
-- 			locate('47-244641',pn)>0 or locate('47-251922',pn)>0 or locate('47-273702',pn)>0,0,            
			locate('999999999',pn)>0,0,            
			least(
				quantity_inflow,
				if(mount_1_qty_needed>0,floor(mount_1_inflow_qty/mount_1_qty_needed),999)+if(mount_1_needed=backup_mount_1_needed,0,if(backup_mount_1_qty_needed>0,floor(backup_mount_1_inflow_qty/backup_mount_1_qty_needed),0)),
				if(mount_2_qty_needed>0,floor(mount_2_inflow_qty/mount_2_qty_needed),999)+if(mount_2_needed=backup_mount_2_needed,0,if(backup_mount_2_qty_needed>0,floor(backup_mount_2_inflow_qty/backup_mount_2_qty_needed),0)),
				if(mount_3_qty_needed>0,floor(mount_3_inflow_qty/mount_3_qty_needed),999)
				)) as assem_6112_adjust_for_mounts
	from carawdata.unique_6112_with_cost as cc
	;
/* STACK INFLOW DATA WITH CIRCLIP 6112 PART NUMBERS THAT NOW INCLUDE STOCK AND COST */
drop table if exists carawdata.inFlow_stack_with_assem_data ;
create table carawdata.inFlow_stack_with_assem_data
	(name_inflow varchar(150),quantity_inflow double,cost_inflow float,part_number_common varchar(100))
    ;
insert ignore carawdata.inFlow_stack_with_assem_data
    (select name_inflow,quantity_inflow,cost_inflow,part_number_common from carawdata.soss_inFlow_StockLevels_00) union all
    (select pn,assem_6112_adjust_for_mounts,assembled_6112_cost,pn_assem_common from carawdata.assem_6112_stock_adjust_for_mounts)
	;
/* CHECK & DEBUG */
--     select * from carawdata.inFlow_stack_with_assem_data order by name_inflow ;
/* END AUGMENTATION OF 6112 ASSEMBLED DATA */
/* END AUGMENTATION OF 6112 ASSEMBLED DATA */
/* END AUGMENTATION OF 6112 ASSEMBLED DATA */
/* SLICK FOR MERGE */
drop table if exists carawdata.soss_inFlow_StockLevels_slick ;
create table carawdata.soss_inFlow_StockLevels_slick
	(name_inflow varchar(150),inFlow_pn1_qty double,inFlow_pn1_cost float,inflow_part_number_common varchar(100))
	;
insert ignore carawdata.soss_inFlow_StockLevels_slick
	select
		trim(name_inflow) as name_inflow,
        /* THIS CODE ZEROES-OUT BASE PART 6112 IN INFLOW, SO IT IS NOT AVAILABLE AS UNASSEMBLED KIT */
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */
        /* ALLOWING MOST BASE PARTS TO SOURCE FROM SOSSINFLOW */
--         if(left(ucase(name_inflow),3)='BIL' and locate('cc',lcase(name_inflow))=0 and (locate('46-',name_inflow)>0 or locate('47-',name_inflow)>0),0,
        if(left(ucase(name_inflow),3)='BIL' and locate('cc',lcase(name_inflow))=0 and 
			(locate('46-206084',name_inflow)>0 or 
            locate('46-241627',name_inflow)>0 or 
            locate('47-255074',name_inflow)>0 or 
            locate('47-234413',name_inflow)>0 or 
            locate('47-253179',name_inflow)>0)
            ,0,
			(case
				when locate('46-206084_cc',name_inflow)>0 then least(4,quantity_inflow)
				when locate('46-241627_cc',name_inflow)>0 then least(4,quantity_inflow)
				when locate('47-255074_cc',name_inflow)>0 then least(4,quantity_inflow)
				when locate('47-234413_cc',name_inflow)>0 then least(4,quantity_inflow)
				when locate('47-253179_cc',name_inflow)>0 then least(4,quantity_inflow)
				else quantity_inflow
			end)) as inFlow_pn1_qty,
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */
        /* DE-STOCK DESTOCK DE STOCK 6112 BASE PARTS HERE */            
        cost_inflow as inFlow_pn1_cost,
        part_number_common as inflow_part_number_common
	from carawdata.inFlow_stack_with_assem_data
    order by name_inflow
    ;
create index iiinFlow on carawdata.soss_inFlow_StockLevels_slick(inflow_part_number_common);
alter table carawdata.soss_inFlow_StockLevels_slick convert to character set utf8mb4 collate utf8mb4_0900_ai_ci ;
/* DEBUG */
-- 	select * from carawdata.soss_inFlow_StockLevels_slick ;
-- 	select * from carawdata.soss_inFlow_StockLevels_slick 
-- 		where locate('46-241627',lcase(name_inflow))>0 or locate('46-206084',lcase(name_inflow))>0 
-- 		order by name_inflow 
-- 		;
-- 	select * from carawdata.soss_inFlow_StockLevels_slick where inflow_pn1_cost<1 or trim(inflow_pn1_cost)='' ;

/* JOIN THE CA INVENTORY NUMBER ON FROM PRODUCT SET FOR UP-MATCH TO CA VIA API */
-- select * from carawdata.pset_assem_only ;
-- select * from carawdata.soss_product_set_lookup limit 1000 ;

drop table if exists carawdata.inflow_update_prcs_skujoin ;
create table carawdata.inflow_update_prcs_skujoin
	(ca_inventory_number varchar(60),stdzd_pn varchar(250),name_inflow varchar(99),inflow_pn1_qty float)
    ;
insert ignore carawdata.inflow_update_prcs_skujoin
	select
		ca_inventory_number,
        stdzd_pn,
        ifnull(name_inflow,'') as name_inflow,
        ifnull(inflow_pn1_qty,0) as inflow_pn1_qty
        /* THIS IS WHERE SOSS ESCROW CAN GET FED !!! */
	from carawdata.soss_product_set_lookup as pset
    left join carawdata.soss_inFlow_StockLevels_slick as inf
    on pset.stdzd_pn=concat(inf.name_inflow,'.1')
    where 
		locate(' ',stdzd_pn)=0 and 
        right(stdzd_pn,2)='.1'
	group by ca_inventory_number
	;
    
/* FINAL TABLE FOR SENDING TO CHANNEL ADVISOR */
drop table if exists carawdata.inflow_update_augmented_chnladv_ready ;
create table carawdata.inflow_update_augmented_chnladv_ready 
	(ca_inventory_number text,partnumber_with_linecode text,sossinflow_qty_to_send float,sossescrow_qty_to_send float)
    ;
insert ignore carawdata.inflow_update_augmented_chnladv_ready 
	select
		ca_inventory_number,
        name_inflow as partnumber_with_linecode,
        inflow_pn1_qty as sossinflow_qty,
        0 as sossescrow_qty_to_send
	from carawdata.inflow_update_prcs_skujoin
	;
	select * from carawdata.inflow_update_augmented_chnladv_ready ;


/* CLEAN UP */
drop table if exists carawdata.soss_inFlow_StockLevels_slick ;
drop table if exists carawdata.inFlow_stack_with_assem_data ;
drop table if exists carawdata.assem_6112_stock_adjust_for_mounts ;
drop table if exists carawdata.unique_6112_with_cost ;
drop table if exists carawdata.unique_6112_partnumbers ;
drop table if exists carawdata.pset_assem_only ;
drop table if exists carawdata.soss_inFlow_StockLevels_00 ;
drop table if exists carawdata.inflow_live_inventory_trim ;
drop table if exists carawdata.inFlow_stack_with_assem_data ;
drop table if exists carawdata.assem_6112_stock_adjust_for_mounts ;
drop table if exists carawdata.unique_6112_with_cost ;
drop table if exists carawdata.unique_6112_partnumbers ;
drop table if exists carawdata.pset_assem_only ;
drop table if exists carawdata.soss_inFlow_StockLevels_00 ;
drop table if exists carawdata.inflow_live_inventory_trim ;