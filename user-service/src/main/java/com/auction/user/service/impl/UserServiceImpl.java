package com.auction.user.service.impl;

import com.auction.user.controller.dto.*;
import com.auction.user.domain.entity.User;
import com.auction.user.domain.enums.RoleEnum;
import com.auction.user.exception.BusinessException;
import com.auction.user.repository.UserMapper;
import com.auction.user.security.JwtTokenProvider;
import com.auction.user.service.UserService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class UserServiceImpl implements UserService {

    private final UserMapper userMapper;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider jwtTokenProvider;
    private final StringRedisTemplate stringRedisTemplate;

    public UserServiceImpl(UserMapper userMapper,
                           PasswordEncoder passwordEncoder,
                           JwtTokenProvider jwtTokenProvider,
                           StringRedisTemplate stringRedisTemplate) {
        this.userMapper = userMapper;
        this.passwordEncoder = passwordEncoder;
        this.jwtTokenProvider = jwtTokenProvider;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public UserResponse register(RegisterRequest request) {
        // Check username uniqueness
        Long usernameCount = userMapper.selectCount(
                new LambdaQueryWrapper<User>().eq(User::getUsername, request.getUsername()));
        if (usernameCount > 0) {
            throw new BusinessException(409, "Username already exists");
        }

        // Check email uniqueness
        Long emailCount = userMapper.selectCount(
                new LambdaQueryWrapper<User>().eq(User::getEmail, request.getEmail()));
        if (emailCount > 0) {
            throw new BusinessException(409, "Email already exists");
        }

        User user = new User();
        user.setUsername(request.getUsername());
        user.setEmail(request.getEmail());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setRole(RoleEnum.USER.name());

        userMapper.insert(user);

        return UserResponse.fromEntity(user);
    }

    @Override
    public LoginResponse login(LoginRequest request) {
        User user = userMapper.selectOne(
                new LambdaQueryWrapper<User>().eq(User::getUsername, request.getUsername()));

        if (user == null || !passwordEncoder.matches(request.getPassword(), user.getPasswordHash())) {
            throw new BusinessException(401, "Invalid credentials");
        }

        Set<String> roles = Set.of(user.getRole());
        String token = jwtTokenProvider.createAccessToken(user.getId(), user.getUsername(), roles);

        return LoginResponse.builder()
                .token(token)
                .tokenType("Bearer")
                .expiresIn(jwtTokenProvider.getAccessTokenTtlSeconds())
                .build();
    }

    @Override
    public void logout(String jti, long remainingTtlSeconds) {
        if (remainingTtlSeconds > 0) {
            stringRedisTemplate.opsForValue().set(
                    "token:blacklist:" + jti, "1", remainingTtlSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public UserResponse getProfile(Long userId) {
        User user = userMapper.selectById(userId);
        if (user == null) {
            throw new BusinessException(404, "User not found");
        }
        return UserResponse.fromEntity(user);
    }

    @Override
    public UserResponse updateProfile(Long userId, UpdateProfileRequest request) {
        User user = userMapper.selectById(userId);
        if (user == null) {
            throw new BusinessException(404, "User not found");
        }

        // Check email uniqueness (exclude self)
        if (!user.getEmail().equals(request.getEmail())) {
            Long emailCount = userMapper.selectCount(
                    new LambdaQueryWrapper<User>()
                            .eq(User::getEmail, request.getEmail())
                            .ne(User::getId, userId));
            if (emailCount > 0) {
                throw new BusinessException(409, "Email already exists");
            }
        }

        user.setEmail(request.getEmail());
        userMapper.updateById(user);

        return UserResponse.fromEntity(user);
    }
}
