package com.kyj.fmk.sec.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kyj.fmk.core.exception.custom.KyjBizException;
import com.kyj.fmk.core.exception.custom.KyjSysException;
import com.kyj.fmk.core.model.enm.CmErrCode;
import com.kyj.fmk.sec.dto.member.LogoutKafkaDTO;
import com.kyj.fmk.sec.dto.oauth2.CustomOAuth2User;
import com.kyj.fmk.sec.dto.res.SecurityResponse;
import com.kyj.fmk.sec.service.TokenService;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.stereotype.Component;

import java.io.IOException;
/**
 *  * 2025-08-09
 *  * @author 김용준
 *  * 스프링 시큐리티에서 사용되는 로그아웃에 성공하였을때 실행되는 핸들러이다.
 *  */
@RequiredArgsConstructor
@Component
public class CustomLogoutSuccessHandler implements LogoutSuccessHandler {

    private final KafkaTemplate<String,String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    @Override
    public void onLogoutSuccess(HttpServletRequest request, HttpServletResponse response, Authentication authentication) throws IOException, ServletException {
        SecurityResponse.writeSuccessRes(response);

        String data  = null;
        CustomOAuth2User customOAuth2User = (CustomOAuth2User)authentication;

        String usrSeqId = String.valueOf(customOAuth2User.getUsrSeqId());

        LogoutKafkaDTO logoutKafkaDTO = new LogoutKafkaDTO();
        logoutKafkaDTO.setUsrSeqId(usrSeqId);
        try {

            data = objectMapper.writeValueAsString(logoutKafkaDTO);

        } catch (JsonProcessingException e) {
            throw new KyjSysException(CmErrCode.CM016);
        }

        if(data == null){
            throw new KyjBizException(CmErrCode.CM019);

        }

        kafkaTemplate.send(logoutKafkaDTO.getTopic(),data);
    }

}
