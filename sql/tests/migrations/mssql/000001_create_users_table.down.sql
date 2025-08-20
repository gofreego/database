-- Drop foreign key constraint first
IF OBJECT_ID('FK_posts_user_id', 'F') IS NOT NULL
    ALTER TABLE posts DROP CONSTRAINT FK_posts_user_id;

-- Drop indexes
IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_users_active' AND object_id = OBJECT_ID('users'))
    DROP INDEX idx_users_active ON users;

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_users_email' AND object_id = OBJECT_ID('users'))
    DROP INDEX idx_users_email ON users;

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_posts_published_at' AND object_id = OBJECT_ID('posts'))
    DROP INDEX idx_posts_published_at ON posts;

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_posts_status' AND object_id = OBJECT_ID('posts'))
    DROP INDEX idx_posts_status ON posts;

IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'idx_posts_user_id' AND object_id = OBJECT_ID('posts'))
    DROP INDEX idx_posts_user_id ON posts;

-- Drop tables
IF OBJECT_ID('posts', 'U') IS NOT NULL
    DROP TABLE posts;

IF OBJECT_ID('users', 'U') IS NOT NULL
    DROP TABLE users; 