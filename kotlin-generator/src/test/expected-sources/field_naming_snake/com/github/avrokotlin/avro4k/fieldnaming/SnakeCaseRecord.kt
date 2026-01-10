@file:OptIn(
    InternalAvro4kApi::class,
    ExperimentalAvro4kApi::class,
)

package com.github.avrokotlin.avro4k.fieldnaming

import com.github.avrokotlin.avro4k.AvroAlias
import com.github.avrokotlin.avro4k.AvroDefault
import com.github.avrokotlin.avro4k.AvroDoc
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.InternalAvro4kApi
import com.github.avrokotlin.avro4k.`internal`.AvroGenerated
import kotlin.Int
import kotlin.OptIn
import kotlin.String
import kotlin.jvm.JvmInline
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
@AvroGenerated("""{"type":"record","name":"SnakeCaseRecord","namespace":"com.github.avrokotlin.avro4k.fieldnaming","fields":[{"name":"userId","type":"int","doc":"User identifier","default":0,"aliases":["user_identifier"]},{"name":"billingAddress","type":{"type":"record","name":"BillingAddress","fields":[{"name":"streetLine","type":"string","doc":"Street line","default":"unknown"}]},"doc":"Full billing address"},{"name":"accountStatus","type":["string",{"type":"enum","name":"StatusFlag","symbols":["ACTIVE","INACTIVE"]},"null"],"doc":"Status wrapper","aliases":["status_alias"]}]}""")
public data class SnakeCaseRecord(
    /**
     * User identifier
     *
     * Default value: 0
     */
    @AvroDoc("User identifier")
    @AvroAlias("user_identifier")
    @AvroDefault("0")
    @SerialName("userId")
    public val user_id: Int = 0,
    /**
     * Full billing address
     */
    @AvroDoc("Full billing address")
    @SerialName("billingAddress")
    public val billing_address: BillingAddress,
    /**
     * Status wrapper
     */
    @AvroDoc("Status wrapper")
    @AvroAlias("status_alias")
    @SerialName("accountStatus")
    public val account_status: AccountStatusUnion? = null,
) {
    @Serializable
    public sealed interface AccountStatusUnion {
        @JvmInline
        @Serializable
        public value class ForString(
            public val `value`: String,
        ) : AccountStatusUnion

        @JvmInline
        @Serializable
        public value class ForStatusFlag(
            public val `value`: StatusFlag,
        ) : AccountStatusUnion
    }
}
