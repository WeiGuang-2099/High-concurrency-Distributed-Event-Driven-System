package com.auction.user.service;

import com.auction.user.controller.dto.*;

public interface UserService {

    UserResponse register(RegisterRequest request);

    LoginResponse login(LoginRequest request);

    void logout(String jti, long remainingTtlSeconds);

    UserResponse getProfile(Long userId);

    UserResponse updateProfile(Long userId, UpdateProfileRequest request);
}
