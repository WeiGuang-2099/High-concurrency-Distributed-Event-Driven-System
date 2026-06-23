package com.auction.user.service;

import com.auction.common.exception.BusinessException;
import com.auction.user.controller.dto.LoginRequest;
import com.auction.user.controller.dto.LoginResponse;
import com.auction.user.controller.dto.RegisterRequest;
import com.auction.user.controller.dto.UpdateProfileRequest;
import com.auction.user.controller.dto.UserResponse;
import com.auction.user.domain.entity.User;
import com.auction.user.repository.UserMapper;
import com.auction.user.security.JwtTokenProvider;
import com.auction.user.service.impl.UserServiceImpl;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("UserService - 用户注册/登录/资料管理")
class UserServiceImplTest {

    @Mock private UserMapper userMapper;
    @Mock private PasswordEncoder passwordEncoder;
    @Mock private JwtTokenProvider jwtTokenProvider;
    @Mock private StringRedisTemplate stringRedisTemplate;
    @Mock private ValueOperations<String, String> valueOperations;

    @InjectMocks private UserServiceImpl userService;

    private RegisterRequest registerRequest;

    @BeforeEach
    void setUp() {
        registerRequest = new RegisterRequest();
        registerRequest.setUsername("testuser");
        registerRequest.setEmail("test@example.com");
        registerRequest.setPassword("password123");
    }

    // ==================== register ====================

    @Nested
    @DisplayName("register() 用户注册")
    class RegisterTest {

        @Test
        @DisplayName("正常注册 - 用户名和邮箱都不重复时注册成功")
        void register_success() {
            when(userMapper.selectCount(any(LambdaQueryWrapper.class))).thenReturn(0L);
            when(passwordEncoder.encode("password123")).thenReturn("encoded_hash");

            UserResponse response = userService.register(registerRequest);

            assertThat(response).isNotNull();
            assertThat(response.getUsername()).isEqualTo("testuser");
            assertThat(response.getEmail()).isEqualTo("test@example.com");
            verify(userMapper).insert(any(User.class));
        }

        @Test
        @DisplayName("用户名已存在 - 抛出409异常")
        void register_usernameExists_throws409() {
            when(userMapper.selectCount(any(LambdaQueryWrapper.class)))
                    .thenReturn(1L)   // username count
                    .thenReturn(0L);  // email count

            assertThatThrownBy(() -> userService.register(registerRequest))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));

            verify(userMapper, never()).insert(any());
        }

        @Test
        @DisplayName("邮箱已存在 - 抛出409异常")
        void register_emailExists_throws409() {
            when(userMapper.selectCount(any(LambdaQueryWrapper.class)))
                    .thenReturn(0L)   // username count
                    .thenReturn(1L);  // email count

            assertThatThrownBy(() -> userService.register(registerRequest))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));

            verify(userMapper, never()).insert(any());
        }

        @Test
        @DisplayName("密码应被加密存储")
        void register_passwordIsHashed() {
            when(userMapper.selectCount(any(LambdaQueryWrapper.class))).thenReturn(0L);
            when(passwordEncoder.encode("password123")).thenReturn("ENCODED_HASH");

            userService.register(registerRequest);

            verify(passwordEncoder).encode("password123");
            verify(userMapper).insert(argThat(user ->
                    "ENCODED_HASH".equals(user.getPasswordHash())));
        }

        @Test
        @DisplayName("新用户默认角色为 USER")
        void register_defaultRoleIsUser() {
            when(userMapper.selectCount(any(LambdaQueryWrapper.class))).thenReturn(0L);
            when(passwordEncoder.encode(any())).thenReturn("hash");

            userService.register(registerRequest);

            verify(userMapper).insert(argThat(user -> "USER".equals(user.getRole())));
        }
    }

    // ==================== login ====================

    @Nested
    @DisplayName("login() 用户登录")
    class LoginTest {

        @Test
        @DisplayName("正常登录 - 凭证正确时返回 JWT token")
        void login_success() {
            User user = new User();
            user.setId(1L);
            user.setUsername("testuser");
            user.setPasswordHash("encoded_hash");
            user.setRole("USER");

            LoginRequest loginRequest = new LoginRequest();
            loginRequest.setUsername("testuser");
            loginRequest.setPassword("password123");

            when(userMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(user);
            when(passwordEncoder.matches("password123", "encoded_hash")).thenReturn(true);
            when(jwtTokenProvider.createAccessToken(eq(1L), eq("testuser"), any())).thenReturn("jwt.token.here");
            when(jwtTokenProvider.getAccessTokenTtlSeconds()).thenReturn(3600L);

            LoginResponse response = userService.login(loginRequest);

            assertThat(response).isNotNull();
            assertThat(response.getToken()).isEqualTo("jwt.token.here");
            assertThat(response.getTokenType()).isEqualTo("Bearer");
            assertThat(response.getExpiresIn()).isEqualTo(3600L);
        }

        @Test
        @DisplayName("用户不存在 - 抛出401异常")
        void login_userNotFound_throws401() {
            LoginRequest loginRequest = new LoginRequest();
            loginRequest.setUsername("nouser");
            loginRequest.setPassword("password");

            when(userMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(null);

            assertThatThrownBy(() -> userService.login(loginRequest))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(401));

            verify(jwtTokenProvider, never()).createAccessToken(anyLong(), anyString(), any());
        }

        @Test
        @DisplayName("密码错误 - 抛出401异常")
        void login_wrongPassword_throws401() {
            User user = new User();
            user.setId(1L);
            user.setUsername("testuser");
            user.setPasswordHash("encoded_hash");
            user.setRole("USER");

            LoginRequest loginRequest = new LoginRequest();
            loginRequest.setUsername("testuser");
            loginRequest.setPassword("wrong");

            when(userMapper.selectOne(any(LambdaQueryWrapper.class))).thenReturn(user);
            when(passwordEncoder.matches("wrong", "encoded_hash")).thenReturn(false);

            assertThatThrownBy(() -> userService.login(loginRequest))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(401));

            verify(jwtTokenProvider, never()).createAccessToken(anyLong(), anyString(), any());
        }
    }

    // ==================== getProfile ====================

    @Nested
    @DisplayName("getProfile() 获取用户资料")
    class GetProfileTest {

        @Test
        @DisplayName("正常获取用户资料")
        void getProfile_success() {
            User user = new User(1L, "testuser", "test@example.com", "hash", "USER", null, null);

            when(userMapper.selectById(1L)).thenReturn(user);

            UserResponse response = userService.getProfile(1L);

            assertThat(response).isNotNull();
            assertThat(response.getId()).isEqualTo(1L);
            assertThat(response.getUsername()).isEqualTo("testuser");
        }

        @Test
        @DisplayName("用户不存在 - 抛出404异常")
        void getProfile_notFound_throws404() {
            when(userMapper.selectById(999L)).thenReturn(null);

            assertThatThrownBy(() -> userService.getProfile(999L))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(404));
        }
    }

    // ==================== updateProfile ====================

    @Nested
    @DisplayName("updateProfile() 更新用户资料")
    class UpdateProfileTest {

        @Test
        @DisplayName("正常更新邮箱")
        void updateProfile_success() {
            User user = new User(1L, "testuser", "old@example.com", "hash", "USER", null, null);
            when(userMapper.selectById(1L)).thenReturn(user);
            when(userMapper.selectCount(any(LambdaQueryWrapper.class))).thenReturn(0L);

            UpdateProfileRequest request = new UpdateProfileRequest();
            request.setEmail("new@example.com");

            UserResponse response = userService.updateProfile(1L, request);

            assertThat(response.getEmail()).isEqualTo("new@example.com");
            verify(userMapper).updateById(any(User.class));
        }

        @Test
        @DisplayName("邮箱被其他用户占用 - 抛出409异常")
        void updateProfile_emailTaken_throws409() {
            User user = new User(1L, "testuser", "old@example.com", "hash", "USER", null, null);
            when(userMapper.selectById(1L)).thenReturn(user);
            when(userMapper.selectCount(any(LambdaQueryWrapper.class))).thenReturn(1L);

            UpdateProfileRequest request = new UpdateProfileRequest();
            request.setEmail("taken@example.com");

            assertThatThrownBy(() -> userService.updateProfile(1L, request))
                    .isInstanceOf(BusinessException.class)
                    .satisfies(ex -> assertThat(((BusinessException) ex).getCode()).isEqualTo(409));

            verify(userMapper, never()).updateById(any());
        }

        @Test
        @DisplayName("邮箱未变 - 不检查重复直接更新")
        void updateProfile_sameEmail_skipsCheck() {
            User user = new User(1L, "testuser", "same@example.com", "hash", "USER", null, null);
            when(userMapper.selectById(1L)).thenReturn(user);

            UpdateProfileRequest request = new UpdateProfileRequest();
            request.setEmail("same@example.com");

            userService.updateProfile(1L, request);

            verify(userMapper, never()).selectCount(any());
            verify(userMapper).updateById(any(User.class));
        }
    }

    // ==================== logout ====================

    @Nested
    @DisplayName("logout() 用户登出")
    class LogoutTest {

        @Test
        @DisplayName("正常登出 - token 加入黑名单")
        void logout_success() {
            when(stringRedisTemplate.opsForValue()).thenReturn(valueOperations);

            userService.logout("jti-123", 1800L);

            verify(valueOperations).set(eq("token:blacklist:jti-123"), eq("1"), eq(1800L), eq(TimeUnit.SECONDS));
        }

        @Test
        @DisplayName("token已过期 - 不加入黑名单(剩余时间为0)")
        void logout_expiredToken_notBlacklisted() {
            userService.logout("jti-123", 0L);

            verify(stringRedisTemplate, never()).opsForValue();
        }
    }
}
