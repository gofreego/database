DROP INDEX IF EXISTS idx_users_active;
DROP INDEX IF EXISTS idx_users_email;
DROP INDEX IF EXISTS idx_posts_published_at;
DROP INDEX IF EXISTS idx_posts_status;
DROP INDEX IF EXISTS idx_posts_user_id;

DROP TABLE IF EXISTS posts; 
DROP TABLE IF EXISTS users; 