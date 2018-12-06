-- **************************************************************************
-- Description: Get the Database name to be used based on catalog associated 
-- with the networkId
-- Author: Muhammad Irfan
-- Create Date: Dec, 2017
-- Param: @NetwordId = Id of Netword
-- Return: Database to which network's catalog is associated
-- Modified Date: Feb 04, 2018
-- Modification: Initial Version
-- ***************************************************************************

DROP FUNCTION IF EXISTS GetDatabaseName;
delimiter //
CREATE FUNCTION GetDatabaseName (NetwordId INT)
  RETURNS VARCHAR(60)
   DETERMINISTIC
    BEGIN
	    DECLARE CatalogKey VARCHAR(30) DEFAULT NULL;
	    DECLARE DatabaseName VARCHAR(60) DEFAULT NULL;
	    SET CATALOGKEY =GetCatalogKey(NetwordId);
	    IF (STRCMP(CATALOGKEY,"SPR")=0) THEN 
	    	SET DatabaseName="content_op";
	    end IF;
    	RETURN DatabaseName;
    	END//
delimiter ;

-- select GetDatabaseName(51);

-- **************************************************************************
-- Description: Get the catalog key to which requested Network is associated
-- Author: Muhammad Irfan
-- Create Date: July 22, 2018
-- Param: @NetwordId = Id of Netword
-- Return: catalogkey to which network is associated
-- Modified Date: Sep 22, 2018
-- Modification: Initial Version
-- ***************************************************************************

DROP FUNCTION IF EXISTS GetCatalogKey;
delimiter //
CREATE FUNCTION GetCatalogKey (NetwordId INT)
  RETURNS VARCHAR(30)
   DETERMINISTIC
    BEGIN
	    DECLARE SearchAttribute CHAR(10);
	    DECLARE AttributeIndex,StartIndex,EndIndex INT;
	    DECLARE CatalogKey VARCHAR(30) DEFAULT NULL;
    	DECLARE NetworkProperties VARCHAR(5000);
    	SET SearchAttribute='catalogs=';
    	
    	SELECT properties INTO NETWORKPROPERTIES FROM network WHERE id=NetwordId;
    	SELECT INSTR(NETWORKPROPERTIES,SearchAttribute) INTO AttributeIndex;
    	IF(AttributeIndex >0) then
    		SET StartIndex = AttributeIndex+LENGTH(SearchAttribute);
    		/* Catalog key can be followed by : and ; signs so check for both to get ending index*/
    		SELECT LOCATE(':',NetworkProperties,AttributeIndex) INTO EndIndex;
    		IF (EndIndex < 1 ) THEN
    			SELECT LOCATE(';',NetworkProperties,AttributeIndex) INTO EndIndex;
    		END IF;
    		/* Get the key of catalog that is being used by the requested network */
    		SET CatalogKey = UCASE(SUBSTRING(NetworkProperties,StartIndex,EndIndex-StartIndex));
    	END IF;
    	
    	RETURN CatalogKey;

    	END//
delimiter ;

--select GetCatalogKey(50);

-- **************************************************************************
-- Description: 'ProcessFeatures' Stored Procedure Performs follwoing tasks.
--              i. Identity the networks that are using catalog passed in Spored Procedure.
--			   ii. Call ProcessNetworkFeatures Stored Procedure for each network identified in above step 
-- Author: Muhammad Iran
-- Create Date: Jan 22, 2018
-- Modified Date: Mar 04, 2018
-- Modification: Created separate SP for each network processing
-- ***************************************************************************
DROP PROCEDURE IF EXISTS ProcessFeatures;
delimiter //
CREATE PROCEDURE ProcessFeatures (CatalogKey VARCHAR(30),DatabaseName VARCHAR(60))
	BEGIN
		DECLARE NetwordId INT DEFAULT 0;
		DECLARE doneNetworks INT DEFAULT 0;
		DECLARE NetworkCursor CURSOR FOR SELECT distinct(network_id) FROM product_feature;
		DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET doneNetworks = 1;
		OPEN NetworkCursor;
			allNetworks: LOOP
				FETCH NetworkCursor INTO NetwordId;
    	    	IF doneNetworks THEN
		        	LEAVE allNetworks; 
    			END IF;
    			IF (GetCatalogKey(NetwordId) = CatalogKey) THEN
    				call ProcessNetworkFeatures(NetwordId,DatabaseName);
    			END IF;
    		END LOOP allNetworks;
		CLOSE NetworkCursor;
	END//
delimiter ;

--call ProcessFeatures("spr","content_op");

-- **************************************************************************
-- Description: 'ProcessNetworkFeatures' Stored Procedure Performs follwoing tasks.
--			   ii. populate product_id for all products that are defined against the provided network and with sku and sku type combination using content database passed in parameter
--			  iii. populate processed_feature column for all has_attribute=1 defined against that network networks and if any attribute is not evaluated than that feature is marked as inactive. 	
-- Author: Muhammad Irfan
-- Create Date: July 04, 2018
-- Modified Date: August 08, 2018
-- Modification: Used Temporary table for performance 
-- ***************************************************************************

DROP PROCEDURE IF EXISTS ProcessNetworkFeatures;
delimiter //
CREATE PROCEDURE ProcessNetworkFeatures (NetworkId INT,DatabaseName VARCHAR(60))
	BEGIN
		DECLARE doneNetwork INT DEFAULT 0;
		SELECT CONCAT("Processing Product Features for Network Id: ",NetworkId);
		-- If Database name is not passed get it using network Id
		IF (DatabaseName IS NULL) THEN
			SET DatabaseName = GetDatabaseName(NetworkId);
		END IF;
		
		-- Create Tempory table to populate product_id against sku and sku type
		DROP TABLE IF EXISTS temp_product_info;
		CREATE TEMPORARY TABLE `temp_product_info` (`product_id` int(11),`sku_type` varchar(60) default NULL, `sku` varchar(60) default NULL);
		
		-- Populate product for records defined against sku/skutype
		SET @DynamicQuery=CONCAT('INSERT INTO temp_product_info(`sku_type`,`sku`) (SELECT sku_type,sku FROM product_feature where network_id=',NetworkId,' AND sku_type IS NOT NULL GROUP BY sku_type,sku)');
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		
		-- Populate product_id for records defined against sku/skutype
		SET @DynamicQuery=CONCAT('UPDATE temp_product_info tpi,',DatabaseName,'.productskus ps SET tpi.product_id =ps.productid WHERE  tpi.sku_type = ps.name AND tpi.sku = ps.sku');
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		-- Populate product_id for records defined against sku/skutype
		SET @DynamicQuery=CONCAT('UPDATE product_feature pf,temp_product_info tpi SET pf.product_id = tpi.product_id WHERE pf.network_id =',NetworkId,' AND pf.sku_type = tpi.sku_type AND pf.sku = tpi.sku');
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

		-- Set is active to 1 for all those features that are now have product_id and has_attribute=0 
		SET @DynamicQuery=CONCAT('UPDATE product_feature SET is_active = 1 WHERE network_id =',NetworkId,' AND has_attribute=0 AND sku_type is not null');
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		-- Set processed_feature=feature,is_active=-1(Default state) to all products which need to be populated in next step
		SET @DynamicQuery=CONCAT('UPDATE product_feature SET processed_feature=feature,is_active=-1 WHERE network_id =',NetworkId,' AND product_id > 0 AND has_attribute=1');
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		drop table if exists grouped_values;
		SET @DynamicQuery=CONCAT("create table grouped_values select productid, attributeid, localeid, group_concat(displayvalue) displayvalue from ",DatabaseName,".productattribute group by productid, attributeid, localeid");
		PREPARE stmt FROM @DynamicQuery;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		alter table grouped_values add index (productid, attributeid, localeid);
		
		set @rowsCount =1;
		WHILE (@rowsCount > 0) DO
			-- update processed_feature with latest values of attributes in content Database.
			SET @DynamicQuery=CONCAT("UPDATE product_feature pf left join grouped_values pa on pa.attributeid = (substr(processed_feature, locate('{attribute:', processed_feature)+11, locate('}', processed_feature)-(locate('{attribute:', processed_feature)+11))) and pf.product_id = pa.productid and pf.locale_id = pa.localeid set processed_feature = replace(processed_feature, concat('{attribute:', (substr(processed_feature, locate('{attribute:', processed_feature)+11, locate('}', processed_feature)-(locate('{attribute:', processed_feature)+11))), '}'), pa.displayvalue), is_active = if(pa.displayvalue is null, 0, 1) where network_id=",NetworkId," AND is_active!=0 AND has_attribute=1 AND locate('{attribute:', processed_feature) > 0");
			PREPARE stmt FROM @DynamicQuery;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;

			-- Check if there is any row that is still active and has "{attribute:" in processed_feature and keep processing  in that case
			SET @DynamicCountQuery=CONCAT("SELECT count(1) INTO @rowsCount from product_feature where network_id=",NetworkId," AND is_active=1 AND has_attribute=1 AND locate('{attribute:',processed_feature) >  0");
			PREPARE countStmt FROM @DynamicCountQuery;
			EXECUTE countStmt;
			DEALLOCATE PREPARE countStmt;
		END WHILE;
		
		DROP TABLE IF EXISTS temp_product_info;
	END//
delimiter ;

--call ProcessNetworkFeatures(52,"content_op");