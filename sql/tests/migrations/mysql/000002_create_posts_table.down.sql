-- Drop foreign key constraint first
ALTER TABLE posts DROP FOREIGN KEY posts_ibfk_1;
-- Then drop indexes
DROP INDEX idx_posts_published_at ON posts;
DROP INDEX idx_posts_status ON posts;
DROP INDEX idx_posts_user_id ON posts;
-- Finally drop the table
DROP TABLE IF EXISTS posts; 