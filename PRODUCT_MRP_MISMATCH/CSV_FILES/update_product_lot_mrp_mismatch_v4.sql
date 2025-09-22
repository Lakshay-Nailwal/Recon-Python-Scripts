UPDATE th205.product_lot pl 
        JOIN th205.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (725072,736642,784834,785983,792211,796489,803170,804100,808957,822922,823238,825169,832020,838954,843007,843323,845385,849582,850915,852133,853001,854311,871631,879710);
UPDATE th405.product_lot pl 
        JOIN th405.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (166958,175811,175813,175934);
UPDATE th417.product_lot pl 
        JOIN th417.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (165239,168012,171362,172994,174525,177973,179180,182686,182700,182908,183433,183453,186026);
UPDATE th411.product_lot pl 
        JOIN th411.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (639425,1283570,1309923,1315828);
UPDATE th425.product_lot pl 
        JOIN th425.product p ON pl.product_id = p.id 
        SET pl.mrp = ROUND(
            (pl.old_mrp / (1 + (IFNULL(p.old_gst,0) / 100))) * 
            (1 + ((IFNULL(p.cgst,0) + IFNULL(p.sgst,0)) / 100)), 2
        ),
        pl.updated_on = NOW()
        WHERE pl.id IN (410981,480851,483941,494382,525307,537860,573032,573048);
