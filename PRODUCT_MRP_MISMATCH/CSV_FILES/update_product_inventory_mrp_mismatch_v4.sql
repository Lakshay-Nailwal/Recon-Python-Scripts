UPDATE th405.product_inventory pi 
        JOIN th405.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (209083,223545,229535,229551,229560);
UPDATE th407.product_inventory pi 
        JOIN th407.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (132532);
UPDATE th406.product_inventory pi 
        JOIN th406.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (178657);
UPDATE th417.product_inventory pi 
        JOIN th417.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (189253,197332,205610,209686,215515,220167,220272,221158,224398,231187,232294,234150,235239,238901,239713,240240,240727,241054,242599,244296,244320,244722,244911,245716,245787,245970,246009,246359,246716,246852,247800,247858,249731,249744,250876,250878,251426,251723,251732,251989,252052,252670,252828,252955);
UPDATE th205.product_inventory pi 
        JOIN th205.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (4072688,2977883,1250553,3737553,3651939,3751259,3851496,3862801,3724197,3834276,3826855,3797655,3383148,3199273,3833624,3835557,3831989,3169271,1910264,3385223,1123002,4144458,1917015,1640154,3086366,3469813,2156554,2388698,1361466,1689036,3967853,2714193,4092879,1310637,1293636,1249239,2110219,3857853,3745292,3699912,3700740,3083876,1296047,2321681,3878351,2630778,4139781,4216209,3911107,3274733,3081330,969371,656504,4112488,4175710,4270528,4206811,4281240,3949092,2180820,4268597,4296259,4223505,4204220,4268819,4252293,3935891,4200015,2072181,3938052,4153961,4057787,4096605,3984740,4314028,4316118,4255782,4165478,4211918,3906667,4092755,4314558,4082077,4143905,4152638,4071342,4164853,4320016,4200004,4269854,4194097,4261652,4259462,4196261,4200543,4201370,4264225,3353836,4314109,4316698,3776316,4166929,4081444,4183056,3953468,4200059,3980792,4019145,3980709,4047775,4021014,4020888,4029521,4092116,4030141,4035268,4017304,4011846,4185337,4014355,4012253,4021499,4014011,4205010,4077233,4263616,4307556,4010693,4040014,4020494,4015087,4015167,4029596,4110197,4017301);
UPDATE th425.product_inventory pi 
        JOIN th425.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (108420,180331,180333,187631,193411,230545,275594,300350,337236,337574,371792,390951,400775,400779,400838,400946,413257,413492,413868,419383,420398,423908,435274,436272,447693,452867,457232,457238,457245,457254,458154,468196,470310,470777,470817,471237,475337,483699,483701,483773,484398,484431,484459,485108,485121,490783,490851,490870,490959,491469,497784,499513,501276,505294,508519,516790,516797,516800,516805,517073,518033,518709,521651,521746,521914,526910,531230,536474,537612,552112,567635,571068,574302,587098,600756,628874,665605,669337,683781,694887,695272,696464,706033,750525,758008,762151,785649,796116,820613,820666,824647,824858);
UPDATE th435.product_inventory pi 
        JOIN th435.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (36418);
UPDATE th411.product_inventory pi 
        JOIN th411.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (222217,313385,404234,452261,518561,602730,731555,792497,926851,1061821,1108517,1109848,1117899,1118646,1191773,1192260,1252421,1275087,1281367,1306058,1309918,1312305,1320014,1343503,1363100,1369436,1378346,1386538);
UPDATE th427.product_inventory pi 
        JOIN th427.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (112249,120458,888777);
UPDATE th429.product_inventory pi 
        JOIN th429.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (74987,78455,81385,140002);
UPDATE th214.product_inventory pi 
        JOIN th214.product p ON pi.product_id = p.id 
        SET pi.mrp = ROUND(
            (pi.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pi.updated_on = NOW(),
        pi.dp_updated_at = NOW()
        WHERE pi.id IN (3390576,4004702,4958615);
