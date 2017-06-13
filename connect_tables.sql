delimiter $$

##########################################################################################################################################################################
#this procedure iterates through a table of pit tags data and looks for ambient temperature at the time of the pittags log and the body temperature of the animal logged.
##########################################################################################################################################################################
DROP PROCEDURE IF EXISTS `find ambient and body temperatures for activity log`;
CREATE PROCEDURE `find ambient and body temperatures for activity log` ()
BEGIN

# will hold true if noe values where found for a cursor
DECLARE no_records BOOLEAN DEFAULT FALSE; 

##########
# variables
##########

#variables for the pittags table
DECLARE _pt_id INTEGER;
DECLARE _pt_date DATE;
DECLARE _pt_time TIME;
DECLARE _pt_tag VARCHAR(30);
DECLARE _pt_plate VARCHAR(6);

#for the ibuttons table
DECLARE selected_ta FLOAT;
DECLARE selected_ib_id INTEGER;

#for the body temperature table
DECLARE selected_tb FLOAT;
DECLARE selected_tm_id INTEGER;

#for both tables
DECLARE minutes_diff FLOAT; 

##########
# cursors
##########

#cursor for the pittags data, this cursor will read every line in the table
DECLARE cursorPittags CURSOR FOR SELECT `ID`, PT_DATE, PT_TIME, PT_TAG, PT_PLATE FROM PIT_TAGS;

#cursor for the ibuttons table. The cursor should find all rows with the same date and distance between the IButton log and the pittag log of no more than 30 minutes
DECLARE cursorIbuttons CURSOR FOR SELECT IB_ID, IB_TEMPERATURE, abs(timestampdiff(minute, addtime(concat(IB_DATE, ' 00:00:00'), IB_TIME),addtime(concat(_pt_date, ' 00:00:00'), _pt_time))) as time_diff FROM IBUTTONS WHERE 
	IB_DATE=_pt_date AND 
	abs(timestampdiff(minute, addtime(concat(IB_DATE, ' 00:00:00'), IB_TIME),addtime(concat(_pt_date, ' 00:00:00'), _pt_time))) <31 AND
	IB_PLATE = _pt_plate  
  ORDER BY time_diff;

#cursor for the body temperatures table. The cursor should find all rows with the same individual (based on the INDIVUDUALS table and the IN_HZ and IN_CHIP columns), date and distance between the temperature log and the pittag log of no more than 30 minutes
DECLARE cursorBodyTemperature CURSOR FOR SELECT `ID`, TM_TEMPERATURE, abs(timestampdiff(minute, addtime(concat(TM_DATE, ' 00:00:00'), TM_TIME),addtime(concat(_pt_date, ' 00:00:00'), _pt_time))) as time_diff FROM TEMPERATURE, INDIVIDUALS WHERE 
	IN_CHIP=_pt_tag AND (IN_HZ<=TM_HZ+0.007 AND IN_HZ>=TM_HZ-0.007) AND  
	abs(timestampdiff(minute, addtime(concat(TM_DATE, ' 00:00:00'), TM_TIME),addtime(concat(_pt_date, ' 00:00:00'), _pt_time))) <31 
	ORDER BY time_diff;

 
DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_records := TRUE;

OPEN cursorPittags;

PITTAGS: loop
  FETCH cursorPittags INTO _pt_id, _pt_date, _pt_time, _pt_tag, _pt_plate;
  if no_records then
    close cursorPittags;
    leave PITTAGS;
  end if;
  open cursorIbuttons;
  open cursorBodyTemperature;
  
  #set ibottons data
  set selected_ta=NULL; set selected_ib_id=NULL;
  fetch cursorIbuttons into selected_ib_id, selected_ta, minutes_diff;      
  #check if rows were fetched and update the PIT_TAGS table
  if no_records=FALSE then
	   #update the PIT_TAGS table with the chosen Ta
	    UPDATE PIT_TAGS SET PT_TA=selected_ta, PT_IB_ID=selected_ib_id WHERE PIT_TAGS.ID=_pt_id;
	    close cursorIbuttons;        
  end if;
  
  #set body temperature data
  fetch cursorBodyTemperature into selected_tm_id, selected_tb, minutes_diff; 
  if no_records=FALSE then
	#update the PIT_TAGS table with the chosen TB
	    UPDATE PIT_TAGS SET PT_TB=selected_tb, PT_TM_ID=selected_tm_id WHERE PIT_TAGS.ID=_pt_id;
	    close cursorBodyTemperature; 
  end if;
end loop PITTAGS;

END$$

DELIMITER ;

