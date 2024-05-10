package com.github.avrokotlin.benchmark.gen

import com.github.avrokotlin.benchmark.BadPartner
import com.github.avrokotlin.benchmark.Client
import com.github.avrokotlin.benchmark.Clients
import com.github.avrokotlin.benchmark.EyeColor
import com.github.avrokotlin.benchmark.GoodPartner
import com.github.avrokotlin.benchmark.Partner
import com.github.avrokotlin.benchmark.Stranger

import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset

internal object ClientsGenerator {
    fun populate(obj: Clients, size: Int): Int {
        var approxSize = 14 // {'clients':[]}

        obj.clients = mutableListOf()
        while (approxSize < size) {
            approxSize += appendClient(obj, size - approxSize)
            approxSize += 1 // ,
        }
        return approxSize
    }

    private fun appendClient(uc: Clients, sizeAvailable: Int): Int {
        var expectedSize = 2 // {}
        val u = Client()
        u.id = Math.abs(RandomUtils.nextLong())
        expectedSize += 9 + u.id.toString().length // ,'id':''
        u.index = (RandomUtils.nextInt(0, Int.MAX_VALUE))
        expectedSize += 11 + u.index.toString().length // ,'index':''
        u.guid = (RandomUtils.nextUUID())
        expectedSize += 10 + 36 // ,'guid':''
        u.isActive = (RandomUtils.nextInt(0, 2) == 1)
        expectedSize += 17 + if (u.isActive) 4 else 5 // ,'isActive':''
        u.balance = (RandomUtils.randomBigDecimal())
        expectedSize += 16 + u.balance!!.toPlainString().length // ,'balance':''
        u.picture = RandomUtils.randomBytes(4048)
        expectedSize += 16 + u.picture!!.size // ,'picture':''
        u.age = (RandomUtils.nextInt(0, 100))
        expectedSize += 9 + u.age.toString().length // ,'age':''
        u.eyeColor = (EyeColor.entries[RandomUtils.nextInt(3)])
        expectedSize += 17 + u.eyeColor!!.name.length // ,'eyeColor':''
        u.name = (RandomUtils.randomAlphanumeric(20))
        expectedSize += 10 + u.name!!.length // ,'name':''
        u.gender = (RandomUtils.randomAlphanumeric(20))
        expectedSize += 12 + u.gender!!.length // ,'gender':''
        u.company = (RandomUtils.randomAlphanumeric(20))
        expectedSize += 13 + u.company!!.length // ,'company':''
        u.emails = (RandomUtils.stringArray(RandomUtils.nextInt(10), 20))
        var calcSize = 0
        for (e in u.emails) {
            calcSize += 3 + e.length
        }
        expectedSize += 11 + calcSize // ,'email':''
        u.phones = (RandomUtils.longArray(RandomUtils.nextInt(10)))
        calcSize = 0
        for (p in u.phones) {
            calcSize += 1 + p.toString().length
        }
        expectedSize += 11 + calcSize // ,'phone':''
        u.address = (RandomUtils.randomAlphanumeric(20))
        expectedSize += 13 + u.address!!.length // ,'address':''
        u.about = (RandomUtils.randomAlphanumeric(20))
        expectedSize += 11 + u.about!!.length // ,'about':''
        u.registered = (
                LocalDate.of(
                    1900 + RandomUtils.nextInt(110),
                    1 + RandomUtils.nextInt(12),
                    1 + RandomUtils.nextInt(28)
                )
                )
        expectedSize += 16 + 10 // ,'registered':''
        u.latitude = (RandomUtils.nextDouble(0.0, 90.0))
        expectedSize += 14 + u.latitude.toString().length // ,'latitude':''
        u.longitude = (RandomUtils.nextDouble(0.0, 180.0))
        expectedSize += 15 + u.longitude.toString().length // ,'longitude':''
        val tags = mutableListOf<String>()
        expectedSize += 10 // ,'tags':[]
        val nTags: Int = RandomUtils.nextInt(0, 50)
        for (i in 0 until nTags) {
            if (expectedSize > sizeAvailable) {
                break
            }
            val t: String = RandomUtils.randomAlphanumeric(10)
            tags.add(t)
            expectedSize += t.length // '',
        }
        u.tags = tags
        val nPartners: Int = RandomUtils.nextInt(0, 30)
        val partners = mutableListOf<Partner?>()
        expectedSize += 13 // ,'partners':[]
        for (i in 0 until nPartners) {
            if (expectedSize > sizeAvailable) {
                break
            }
            val id: Long = RandomUtils.nextLong()
            val name: String = RandomUtils.randomAlphabetic(30)
            val at = OffsetDateTime.of(
                1900 + RandomUtils.nextInt(110),
                1 + RandomUtils.nextInt(12),
                1 + RandomUtils.nextInt(28),
                RandomUtils.nextInt(24),
                RandomUtils.nextInt(60),
                RandomUtils.nextInt(60),
                RandomUtils.nextInt(1000000000),
                ZoneOffset.UTC
            ).toInstant()
            partners.add(
                when (RandomUtils.nextInt(4)) {
                    0 -> GoodPartner(id, name, at)
                    1 -> BadPartner(id, name, at)
                    2 -> if (RandomUtils.nextBoolean()) Stranger.KNOWN_STRANGER else Stranger.UNKNOWN_STRANGER
                    3 -> null
                    else -> throw IllegalStateException("Unexpected value")
                }
            )
            expectedSize += id.toString().length + name.length + 50 // {'id':'','name':'','since':''},
        }
        u.partners = partners
        uc.clients.add(u)
        return expectedSize
    }
}

