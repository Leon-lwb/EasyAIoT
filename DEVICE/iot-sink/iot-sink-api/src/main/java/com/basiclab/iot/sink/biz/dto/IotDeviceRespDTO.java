package com.basiclab.iot.sink.biz.dto;

import lombok.Data;

/**
 * IotDeviceRespDTO
 *
 * @author 翱翔的雄库鲁
 * @email andywebjava@163.com
 * @wechat EasyAIoT2025
 */

@Data
public class IotDeviceRespDTO {

    /**
     * 设备编号
     */
    private Long id;
    /**
     * 产品唯一标识
     */
    private String productIdentification;
    /**
     * 设备唯一标识
     */
    private String deviceIdentification;
    /**
     * 协议类型
     */
    private String protocolType;
    /**
     * 设备 IP 地址
     */
    private String ipAddress;
    /**
     * 设备扩展配置 JSON
     */
    private String extension;
    /**
     * 租户编号
     */
    private Long tenantId;

}
