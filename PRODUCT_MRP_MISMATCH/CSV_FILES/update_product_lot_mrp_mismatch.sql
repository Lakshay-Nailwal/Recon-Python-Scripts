UPDATE th205.product_lot pl 
        JOIN th205.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (702752,713536,721899,746437,753681,758467,768477,777337,778431,779938,807852,808874,810552,816445,817378,836260,846467,849258,853349,857691,857816,868844,872113,873376,874678,876255);
UPDATE th214.product_lot pl 
        JOIN th214.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (1631403,1660420,1700047,1640915,1672599,1632058,1095190,1706350,1639885,1657816,1240350,1546504);
UPDATE th401.product_lot pl 
        JOIN th401.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (268769,276654,290401,292734,293993);
UPDATE th403.product_lot pl 
        JOIN th403.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (118313,129687);
UPDATE th402.product_lot pl 
        JOIN th402.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (706379,285676,733255,724074,735130);
UPDATE th405.product_lot pl 
        JOIN th405.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (157655);
UPDATE th407.product_lot pl 
        JOIN th407.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (161133);
UPDATE th409.product_lot pl 
        JOIN th409.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (283717);
UPDATE th418.product_lot pl 
        JOIN th418.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (131926,136437);
UPDATE th224.product_lot pl 
        JOIN th224.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (1700119);
UPDATE th413.product_lot pl 
        JOIN th413.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (107672,140621,160171,163458,163600,163727,168970,172831,177059,178410,180225,186405,186707,186817,189835,190458,190663,191449,192189,192230,193946,194462,194483,194847,195994,196602,196992,197511,197543,198567,199107,200529,200857,201628,202073,202084,202119,202236,202376,203049,203219,204059,204108,204109,204114,204117,204189,204198,204213,204220,204458,204700,204705);
UPDATE th417.product_lot pl 
        JOIN th417.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (9309,154751,156609,170685,171775,174120,177053,177941,180288,184663,185888,186069,186387,187199);
UPDATE th424.product_lot pl 
        JOIN th424.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (66065,82031,86825,89742,90222,90831);
UPDATE th434.product_lot pl 
        JOIN th434.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (1013);
UPDATE th411.product_lot pl 
        JOIN th411.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (10476,1242839,1244166,1252160,1288479,1291082,1295692,1299979,1301150,1307031,1310714,1312639,1316558,1316991,1320177,1321086,1321721,1322342,1324473,1325126);
UPDATE th430.product_lot pl 
        JOIN th430.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (160702,161654,189268,189294,190054,190056,192563,192940,192944,194445,194449,194463,194465,194466,194472,198246);
UPDATE th435.product_lot pl 
        JOIN th435.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (279437,396912,398622,425025);
UPDATE th410.product_lot pl 
        JOIN th410.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (25329,806881,808483,810184,812741,815911,816653,818404,821337,821861,823011,824707,824839,825265,828927,830802,832060,836288,836540,839529,839669,839991,840870,841786,844616,846129,846283,846503,846762,847131,848641,848884,849085,850501,850619,850675);
UPDATE th425.product_lot pl 
        JOIN th425.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (423725,436166,452696,481109,483330,491730,491997,498612,500620,500889,501497,503872,504051,533449,539658,540544,541663,565302,572004);
UPDATE th427.product_lot pl 
        JOIN th427.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (1160680);
UPDATE th428.product_lot pl 
        JOIN th428.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (823723,838561,852112,856293,856445,878907,878939,889280);
UPDATE th618.product_lot pl 
        JOIN th618.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (2129);
UPDATE th429.product_lot pl 
        JOIN th429.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (1754521,1797466,1797468,1806617);
