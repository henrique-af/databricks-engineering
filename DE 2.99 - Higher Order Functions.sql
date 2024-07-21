
/*Here,
you 'll use the higher order function EXISTS with data from the sales table to create boolean columns mattress 
and pillow that indicate whether the item purchased was a mattress or pillow product.

For example, if item_name from the items column ends with the string "Mattress",
the column value for mattress should be true and the value for pillow should be false. 
Here are a few examples of items and the resulting values.*/

-- TODO
CREATE
OR REPLACE TABLE sales_product_flags AS
SELECT
    items,
    EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
    EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM
    sales