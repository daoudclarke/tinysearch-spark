# The code below shows an example of how to instantiate this type.
# The values are placeholders you should change.
import aws_cdk.aws_emr as emr

# additional_info is of type object
# configuration_property_ is of type ConfigurationProperty
from aws_cdk.core import App


def run():
    app = App()
    cfn_cluster = emr.CfnCluster(
        app, "MyCfnCluster",
        instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
            core_instance_fleet=emr.CfnCluster.InstanceFleetConfigProperty(
                instance_type_configs=[emr.CfnCluster.InstanceTypeConfigProperty(
                    instance_type="instanceType",

                    # the properties below are optional
                    bid_price_as_percentage_of_on_demand_price=0.25,
                    configurations=[emr.CfnCluster.ConfigurationProperty(
                        classification="classification",
                        configuration_properties={
                            "configuration_properties_key": "configurationProperties"
                        },
                        configurations=[configuration_property_]
                    )],
                    ebs_configuration=emr.CfnCluster.EbsConfigurationProperty(
                        ebs_block_device_configs=[emr.CfnCluster.EbsBlockDeviceConfigProperty(
                            volume_specification=emr.CfnCluster.VolumeSpecificationProperty(
                                size_in_gb=123,
                                volume_type="volumeType",

                                # the properties below are optional
                                iops=123
                            ),

                            # the properties below are optional
                            volumes_per_instance=123
                        )],
                        ebs_optimized=False
                    ),
                    weighted_capacity=123
                )],
                launch_specifications=emr.CfnCluster.InstanceFleetProvisioningSpecificationsProperty(
                    on_demand_specification=emr.CfnCluster.OnDemandProvisioningSpecificationProperty(
                        allocation_strategy="allocationStrategy"
                    ),
                    spot_specification=emr.CfnCluster.SpotProvisioningSpecificationProperty(
                        timeout_action="timeoutAction",
                        timeout_duration_minutes=123,

                        # the properties below are optional
                        allocation_strategy="allocationStrategy",
                        block_duration_minutes=123
                    )
                ),
                name="name",
                target_on_demand_capacity=123,
                target_spot_capacity=123
            ),
            core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                instance_count=123,
                instance_type="instanceType",

                # the properties below are optional
                auto_scaling_policy=emr.CfnCluster.AutoScalingPolicyProperty(
                    constraints=emr.CfnCluster.ScalingConstraintsProperty(
                        max_capacity=123,
                        min_capacity=123
                    ),
                    rules=[emr.CfnCluster.ScalingRuleProperty(
                        action=emr.CfnCluster.ScalingActionProperty(
                            simple_scaling_policy_configuration=emr.CfnCluster.SimpleScalingPolicyConfigurationProperty(
                                scaling_adjustment=123,

                                # the properties below are optional
                                adjustment_type="adjustmentType",
                                cool_down=123
                            ),

                            # the properties below are optional
                            market="market"
                        ),
                        name="name",
                        trigger=emr.CfnCluster.ScalingTriggerProperty(
                            cloud_watch_alarm_definition=emr.CfnCluster.CloudWatchAlarmDefinitionProperty(
                                comparison_operator="comparisonOperator",
                                metric_name="metricName",
                                period=123,
                                threshold=123,

                                # the properties below are optional
                                dimensions=[emr.CfnCluster.MetricDimensionProperty(
                                    key="key",
                                    value="value"
                                )],
                                evaluation_periods=123,
                                namespace="namespace",
                                statistic="statistic",
                                unit="unit"
                            )
                        ),

                        # the properties below are optional
                        description="description"
                    )]
                ),
                bid_price="bidPrice",
                configurations=[emr.CfnCluster.ConfigurationProperty(
                    classification="classification",
                    configuration_properties={
                        "configuration_properties_key": "configurationProperties"
                    },
                    configurations=[configuration_property_]
                )],
                ebs_configuration=emr.CfnCluster.EbsConfigurationProperty(
                    ebs_block_device_configs=[emr.CfnCluster.EbsBlockDeviceConfigProperty(
                        volume_specification=emr.CfnCluster.VolumeSpecificationProperty(
                            size_in_gb=123,
                            volume_type="volumeType",

                            # the properties below are optional
                            iops=123
                        ),

                        # the properties below are optional
                        volumes_per_instance=123
                    )],
                    ebs_optimized=False
                ),
                market="market",
                name="name"
            ),
            ec2_key_name="ec2KeyName",
            ec2_subnet_id="ec2SubnetId",
            ec2_subnet_ids=["ec2SubnetIds"],
            emr_managed_master_security_group="emrManagedMasterSecurityGroup",
            emr_managed_slave_security_group="emrManagedSlaveSecurityGroup",
            hadoop_version="hadoopVersion",
            keep_job_flow_alive_when_no_steps=False,
            master_instance_fleet=emr.CfnCluster.InstanceFleetConfigProperty(
                instance_type_configs=[emr.CfnCluster.InstanceTypeConfigProperty(
                    instance_type="instanceType",

                    # the properties below are optional
                    bid_price="bidPrice",
                    bid_price_as_percentage_of_on_demand_price=123,
                    configurations=[emr.CfnCluster.ConfigurationProperty(
                        classification="classification",
                        configuration_properties={
                            "configuration_properties_key": "configurationProperties"
                        },
                        configurations=[configuration_property_]
                    )],
                    ebs_configuration=emr.CfnCluster.EbsConfigurationProperty(
                        ebs_block_device_configs=[emr.CfnCluster.EbsBlockDeviceConfigProperty(
                            volume_specification=emr.CfnCluster.VolumeSpecificationProperty(
                                size_in_gb=123,
                                volume_type="volumeType",

                                # the properties below are optional
                                iops=123
                            ),

                            # the properties below are optional
                            volumes_per_instance=123
                        )],
                        ebs_optimized=False
                    ),
                    weighted_capacity=123
                )],
                launch_specifications=emr.CfnCluster.InstanceFleetProvisioningSpecificationsProperty(
                    on_demand_specification=emr.CfnCluster.OnDemandProvisioningSpecificationProperty(
                        allocation_strategy="allocationStrategy"
                    ),
                    spot_specification=emr.CfnCluster.SpotProvisioningSpecificationProperty(
                        timeout_action="timeoutAction",
                        timeout_duration_minutes=123,

                        # the properties below are optional
                        allocation_strategy="allocationStrategy",
                        block_duration_minutes=123
                    )
                ),
                name="name",
                target_on_demand_capacity=123,
                target_spot_capacity=123
            ),
            master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                instance_count=123,
                instance_type="instanceType",

                # the properties below are optional
                auto_scaling_policy=emr.CfnCluster.AutoScalingPolicyProperty(
                    constraints=emr.CfnCluster.ScalingConstraintsProperty(
                        max_capacity=123,
                        min_capacity=123
                    ),
                    rules=[emr.CfnCluster.ScalingRuleProperty(
                        action=emr.CfnCluster.ScalingActionProperty(
                            simple_scaling_policy_configuration=emr.CfnCluster.SimpleScalingPolicyConfigurationProperty(
                                scaling_adjustment=123,

                                # the properties below are optional
                                adjustment_type="adjustmentType",
                                cool_down=123
                            ),

                            # the properties below are optional
                            market="market"
                        ),
                        name="name",
                        trigger=emr.CfnCluster.ScalingTriggerProperty(
                            cloud_watch_alarm_definition=emr.CfnCluster.CloudWatchAlarmDefinitionProperty(
                                comparison_operator="comparisonOperator",
                                metric_name="metricName",
                                period=123,
                                threshold=123,

                                # the properties below are optional
                                dimensions=[emr.CfnCluster.MetricDimensionProperty(
                                    key="key",
                                    value="value"
                                )],
                                evaluation_periods=123,
                                namespace="namespace",
                                statistic="statistic",
                                unit="unit"
                            )
                        ),

                        # the properties below are optional
                        description="description"
                    )]
                ),
                bid_price="bidPrice",
                configurations=[emr.CfnCluster.ConfigurationProperty(
                    classification="classification",
                    configuration_properties={
                        "configuration_properties_key": "configurationProperties"
                    },
                    configurations=[configuration_property_]
                )],
                ebs_configuration=emr.CfnCluster.EbsConfigurationProperty(
                    ebs_block_device_configs=[emr.CfnCluster.EbsBlockDeviceConfigProperty(
                        volume_specification=emr.CfnCluster.VolumeSpecificationProperty(
                            size_in_gb=123,
                            volume_type="volumeType",

                            # the properties below are optional
                            iops=123
                        ),

                        # the properties below are optional
                        volumes_per_instance=123
                    )],
                    ebs_optimized=False
                ),
                market="market",
                name="name"
            ),
            placement=emr.CfnCluster.PlacementTypeProperty(
                availability_zone="availabilityZone"
            ),
            service_access_security_group="serviceAccessSecurityGroup",
            termination_protected=False
        ),
        job_flow_role="jobFlowRole",
        name="name",
        service_role="serviceRole",

        # the properties below are optional
        additional_info=additional_info,
        applications=[emr.CfnCluster.ApplicationProperty(
            additional_info={
                "additional_info_key": "additionalInfo"
            },
            args=["args"],
            name="name",
            version="version"
        )],
        auto_scaling_role="autoScalingRole",
        bootstrap_actions=[emr.CfnCluster.BootstrapActionConfigProperty(
            name="name",
            script_bootstrap_action=emr.CfnCluster.ScriptBootstrapActionConfigProperty(
                path="path",

                # the properties below are optional
                args=["args"]
            )
        )],
        configurations=[emr.CfnCluster.ConfigurationProperty(
            classification="classification",
            configuration_properties={
                "configuration_properties_key": "configurationProperties"
            },
            configurations=[configuration_property_]
        )],
        custom_ami_id="customAmiId",
        ebs_root_volume_size=123,
        kerberos_attributes=emr.CfnCluster.KerberosAttributesProperty(
            kdc_admin_password="kdcAdminPassword",
            realm="realm",

            # the properties below are optional
            ad_domain_join_password="adDomainJoinPassword",
            ad_domain_join_user="adDomainJoinUser",
            cross_realm_trust_principal_password="crossRealmTrustPrincipalPassword"
        ),
        log_encryption_kms_key_id="logEncryptionKmsKeyId",
        log_uri="logUri",
        managed_scaling_policy=emr.CfnCluster.ManagedScalingPolicyProperty(
            compute_limits=emr.CfnCluster.ComputeLimitsProperty(
                maximum_capacity_units=123,
                minimum_capacity_units=123,
                unit_type="unitType",

                # the properties below are optional
                maximum_core_capacity_units=123,
                maximum_on_demand_capacity_units=123
            )
        ),
        release_label="releaseLabel",
        scale_down_behavior="scaleDownBehavior",
        security_configuration="securityConfiguration",
        step_concurrency_level=123,
        steps=[emr.CfnCluster.StepConfigProperty(
            hadoop_jar_step=emr.CfnCluster.HadoopJarStepConfigProperty(
                jar="jar",

                # the properties below are optional
                args=["args"],
                main_class="mainClass",
                step_properties=[emr.CfnCluster.KeyValueProperty(
                    key="key",
                    value="value"
                )]
            ),
            name="name",

            # the properties below are optional
            action_on_failure="actionOnFailure"
        )],
        tags=[CfnTag(
            key="key",
            value="value"
        )],
        visible_to_all_users=False
    )