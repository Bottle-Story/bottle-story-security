package com.kyj.fmk.sec.dto.member;

import com.kyj.fmk.core.model.KafkaTopic;
import com.kyj.fmk.core.model.dto.BaseKafkaDTO;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogoutKafkaDTO extends BaseKafkaDTO {

    private String usrSeqId;

    public LogoutKafkaDTO(){
        super.setFrom("MEMBER");
        super.setTopic(KafkaTopic.MEMBER_LOGOUT);
    }

}
