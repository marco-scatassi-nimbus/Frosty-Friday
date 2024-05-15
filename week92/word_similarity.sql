use database db_ff;
create or replace schema db_ff.week92;

-- Create a table named fruit_salad
CREATE OR REPLACE TABLE fruit_salad (
    fruits VARCHAR(255)
);

-- Insert sample frutis into the fruit_salad
INSERT INTO fruit_salad (fruits) VALUES
('apple'),
('apricot'),
('banana'),
('pineapple'),
('oranges'),
('kiwi'),
('strawberry'),
('grape'),
('watermelon'),
('pear'),
('peach'),
('strawberry'),
('blueberry'),
('mango'),
('lemon'),
('lime'),
('papaya'),
('cherry'),
('plum'),
('fig'),
('passion fruit'),
('raspberry'),
('blackberry'),
('nectarine'),
('cantaloupe'),
('apricot'),
('tangerine'),
('guava'),
('dragon fruit');


select fruits
    , jarowinkler_similarity(fruits, 'strawberry') as jw_sim_strawberry
from fruit_salad
order by jw_sim_strawberry desc;
